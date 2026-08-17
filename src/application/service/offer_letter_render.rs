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
//! - A token with NO matching variable is left untouched — a visible,
//!   debuggable artifact in the sent letter rather than a silent drop.
//! - Variables themselves are substituted as plain text (no escaping, no
//!   recursion): letter bodies are plain text, not HTML.

/// Render `template`, replacing every `{{token}}` whose name appears in
/// `vars`, leaving unknown tokens as-is.
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
                match vars.get(token) {
                    Some(v) => out.push_str(&json_to_text(v)),
                    // Unknown token: leave `{{token}}` visible in the output.
                    None => {
                        out.push_str("{{");
                        out.push_str(token);
                        out.push_str("}}");
                    }
                }
                rest = &after_open[close + 2..];
            }
        }
    }
    out.push_str(rest);
    out
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
}
