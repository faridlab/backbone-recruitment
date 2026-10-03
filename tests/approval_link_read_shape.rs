//! The generic CRUD reads of the two approval-filed entities carry the
//! approvals-engine link (and the offer its agreed start date).
//!
//! The list/detail endpoints load the row into the generated entity and
//! answer with the generated response DTO, so a column the entity does not
//! declare is silently dropped from every read. These tests stand in for
//! that path without a database: they decode a row-shaped JSON object into
//! the entity exactly as a read would see it, convert to the response DTO,
//! and serialize the HTTP body.

use backbone_recruitment::presentation::dto::{JobOfferResponseDto, JobRequisitionResponseDto};
use backbone_recruitment::{JobOffer, JobRequisition};
use serde_json::json;

const REQUEST_ID: &str = "0ab07a9d-0000-4000-8000-000000000709";

#[test]
fn an_offer_read_returns_its_approval_request_id_and_start_date() {
    let row = json!({
        "id": "3bf7bf48-e855-434c-acc5-51bd4cb9d0ac",
        "approval_request_id": REQUEST_ID,
        "application_id": "11111111-1111-4111-8111-111111111111",
        "proposed_salary": "9000000",
        "employment_type": "permanent",
        "letter_template_id": null,
        "status": "draft",
        "offered_at": null,
        "accepted_at": null,
        "start_date": "2026-11-02",
    });
    let entity: JobOffer = serde_json::from_value(row).expect("row decodes into the entity");
    let body = serde_json::to_value(JobOfferResponseDto::from(entity)).unwrap();

    assert_eq!(
        body["approvalRequestId"], REQUEST_ID,
        "the offer read must carry the approvals link: {body}"
    );
    assert_eq!(body["startDate"], "2026-11-02", "the offer read carries the agreed first day: {body}");
}

#[test]
fn an_unfiled_offer_reads_its_approval_link_as_null() {
    let row = json!({
        "id": "3bf7bf48-e855-434c-acc5-51bd4cb9d0ac",
        "application_id": "11111111-1111-4111-8111-111111111111",
        "status": "draft",
    });
    let entity: JobOffer = serde_json::from_value(row).expect("row decodes into the entity");
    let body = serde_json::to_value(JobOfferResponseDto::from(entity)).unwrap();
    let obj = body.as_object().unwrap();

    assert!(obj.contains_key("approvalRequestId"), "the key is present even when NULL: {body}");
    assert!(body["approvalRequestId"].is_null());
}

#[test]
fn a_requisition_read_returns_its_approval_request_id_and_both_counters() {
    let row = json!({
        "id": "22222222-2222-4222-8222-222222222222",
        "department_id": null,
        "position_id": null,
        "title": "Payroll analyst",
        "headcount": 2,
        "filled_headcount": 2,
        "employment_type": null,
        "status": "open",
        "approval_request_id": REQUEST_ID,
        "opened_by": "33333333-3333-4333-8333-333333333333",
        "budget": null,
        "deadline": null,
    });
    let entity: JobRequisition = serde_json::from_value(row).expect("row decodes into the entity");
    let body = serde_json::to_value(JobRequisitionResponseDto::from(entity)).unwrap();

    assert_eq!(
        body["approvalRequestId"], REQUEST_ID,
        "the requisition read must carry the approvals link: {body}"
    );
    // Openings left = headcount − filledHeadcount: both travel on the read.
    assert_eq!(body["headcount"], 2);
    assert_eq!(body["filledHeadcount"], 2);
}
