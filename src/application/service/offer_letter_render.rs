//! Tiny `{{placeholder}}` renderer for offer-letter templates.
//!
//! The mail module deliberately has no template engine — its templates are
//! raw text — and pulling a full engine in for one letter type is not worth a
//! dependency. Offer letters need exactly "replace named tokens in a
//! template", so that is all this is.
//!
//! Semantics:
//! - `{{ name }}` and `{{name}}` are both recognized (whitespace-tolerant).
//! - A token with a matching variable is replaced by the variable's value.
//! - `{{name|fallback wording}}` renders the fallback when the fact is
//!   absent (no variable, or a null one) — the template author's wording for
//!   an unknown fact, instead of an invented value or a bare token.
//! - A token with NO matching variable and NO fallback is left untouched —
//!   a visible, debuggable artifact in the sent letter rather than a silent
//!   drop.
//! - Variables themselves are substituted as plain text (no escaping, no
//!   recursion): letter bodies are plain text, not HTML.
//!
//! Formal-document values (salary, dates) render through the `format_*`
//! helpers below, never as bare machine values: a letter is read by humans.

use chrono::Datelike;

/// Render `template`, replacing every `{{token}}` whose name appears in
/// `vars`, applying `{{token|fallback}}` wording when the fact is absent,
/// and leaving unknown tokens as-is.
pub fn render(template: &str, vars: &serde_json::Value) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(open) = rest.find("{{") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        match after_open.find("}}") {
            None => {
                // Unterminated token — keep the literal text from here on.
                out.push_str(&rest[open..]);
                return out;
            }
            Some(close) => {
                let token = after_open[..close].trim();
                let (name, fallback) = match token.split_once('|') {
                    Some((n, f)) => (n.trim(), Some(f.trim_start())),
                    None => (token, None),
                };
                match vars.get(name) {
                    Some(v) if !v.is_null() => out.push_str(&json_to_text(v)),
                    // Absent fact (null or unknown) with fallback wording:
                    // render the template author's wording for it.
                    _ if fallback.is_some() => out.push_str(fallback.unwrap_or_default()),
                    // A null variable with no fallback keeps the historical
                    // empty rendering.
                    Some(serde_json::Value::Null) => {}
                    // Unknown token: leave `{{token}}` visible in the output.
                    None => {
                        out.push_str("{{");
                        out.push_str(token);
                        out.push_str("}}");
                    }
                    _ => {}
                }
                rest = &after_open[close + 2..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// Indonesian month names for the written-date form letters carry.
const MONTHS_ID: [&str; 12] = [
    "Januari",
    "Februari",
    "Maret",
    "April",
    "Mei",
    "Juni",
    "Juli",
    "Agustus",
    "September",
    "Oktober",
    "November",
    "Desember",
];

/// Written date form for letters: `27 September 2026`.
pub fn format_date_long(d: chrono::NaiveDate) -> String {
    format!(
        "{} {} {}",
        d.day(),
        MONTHS_ID[(d.month0()) as usize],
        d.year()
    )
}

/// Indonesian currency form for letters: `Rp 12.000.000` — dots for
/// thousands, a comma for cents, and cents only when the amount has them.
pub fn format_salary_idr(d: rust_decimal::Decimal) -> String {
    let whole = d.trunc().abs();
    let digits = whole.to_string();
    let mut grouped = String::with_capacity(digits.len() + digits.len() / 3);
    let len = digits.len();
    for (i, b) in digits.bytes().enumerate() {
        // Groups of three counted from the right: a dot before every digit
        // that starts a complete group.
        if i > 0 && (len - i) % 3 == 0 {
            grouped.push('.');
        }
        grouped.push(b as char);
    }
    // `round()` keeps the Decimal scale, so truncate before stringifying;
    // pad to two digits so half a rupiah reads ",50" and not ",5".
    let cents = (d.fract().abs() * rust_decimal::Decimal::from(100))
        .round()
        .trunc()
        .to_string();
    if cents == "0" {
        format!("Rp {grouped}")
    } else if cents.len() < 2 {
        format!("Rp {grouped},0{cents}")
    } else {
        format!("Rp {grouped},{cents}")
    }
}

/// Stringify a JSON variable for interpolation. Strings lose their quotes;
/// nulls render empty; numbers and booleans render as their JSON text.
fn json_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(v: serde_json::Value) -> serde_json::Value {
        v
    }

    #[test]
    fn replaces_known_tokens_with_and_without_spaces() {
        let t = vars(serde_json::json!({
            "first_name": "Dewi",
            "position": "Staff Accountant"
        }));
        assert_eq!(
            render("Hi {{first_name}} — welcome as {{ position }}!", &t),
            "Hi Dewi — welcome as Staff Accountant!"
        );
    }

    #[test]
    fn leaves_unknown_tokens_visible() {
        let t = vars(serde_json::json!({"known": "x"}));
        assert_eq!(render("{{known}} {{unknown}}", &t), "x {{unknown}}");
    }

    #[test]
    fn null_renders_empty_and_numbers_render_plain() {
        let t = vars(serde_json::json!({"salary": 12000000, "note": null}));
        assert_eq!(render("[{{salary}}][{{note}}]", &t), "[12000000][]");
    }

    #[test]
    fn unterminated_brace_is_kept_literal() {
        let t = vars(serde_json::json!({"a": "b"}));
        assert_eq!(render("{{a}} {{ oops", &t), "b {{ oops");
    }

    #[test]
    fn fallback_wording_renders_when_the_fact_is_absent() {
        let t = vars(serde_json::json!({"known": "x"}));
        assert_eq!(
            render("starts {{start_date|on a date we will agree together}}", &t),
            "starts on a date we will agree together"
        );
    }

    #[test]
    fn fallback_wording_renders_when_the_fact_is_null() {
        let t = vars(serde_json::json!({"start_date": null}));
        assert_eq!(
            render("starts {{start_date|as agreed}}", &t),
            "starts as agreed"
        );
    }

    #[test]
    fn fallback_wording_is_ignored_when_the_fact_is_present() {
        let t = vars(serde_json::json!({"start_date": "27 September 2026"}));
        assert_eq!(
            render("starts {{start_date|as agreed}}", &t),
            "starts 27 September 2026"
        );
    }

    #[test]
    fn a_null_fact_without_fallback_still_renders_empty() {
        let t = vars(serde_json::json!({"note": null}));
        assert_eq!(render("[{{note}}]", &t), "[]");
    }

    #[test]
    fn salary_renders_as_indonesian_currency() {
        use rust_decimal::Decimal;
        assert_eq!(
            format_salary_idr(Decimal::from(12_000_000)),
            "Rp 12.000.000"
        );
        assert_eq!(format_salary_idr(Decimal::from(999)), "Rp 999");
        assert_eq!(format_salary_idr(Decimal::from(1_000)), "Rp 1.000");
        assert_eq!(
            format_salary_idr(Decimal::new(1_000_000_005, 2)),
            "Rp 10.000.000,05"
        );
        assert_eq!(
            format_salary_idr(Decimal::new(1_000_000_050, 2)),
            "Rp 10.000.000,50"
        );
        assert_eq!(
            format_salary_idr(Decimal::new(120_000_000, 1)),
            "Rp 12.000.000"
        );
    }

    #[test]
    fn dates_render_written_out_in_indonesian() {
        assert_eq!(
            format_date_long(chrono::NaiveDate::from_ymd_opt(2026, 9, 27).unwrap()),
            "27 September 2026"
        );
        assert_eq!(
            format_date_long(chrono::NaiveDate::from_ymd_opt(2026, 5, 1).unwrap()),
            "1 Mei 2026"
        );
        assert_eq!(
            format_date_long(chrono::NaiveDate::from_ymd_opt(2026, 8, 8).unwrap()),
            "8 Agustus 2026"
        );
    }
}
