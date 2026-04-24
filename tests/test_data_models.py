from ontology_req_pipeline.data_models import Condition, IndividualRequirement, Record, Span, Structure


def test_record_model_parsing() -> None:
    payload = {
        "idx": 7,
        "original_text": "The valve shall withstand 10 bar.",
        "requirements": [
            {
                "req_idx": 0,
                "raw_text": "The valve shall withstand 10 bar.",
                "structure": {
                    "subject": {"text": "The valve", "start": 0, "end": 9},
                    "modality": "shall",
                    "condition": {
                        "present": False,
                        "EARS_pattern": "ubiquitous",
                        "text": "",
                        "start": 0,
                        "end": 0,
                    },
                    "action": {"text": "shall withstand", "start": 10, "end": 24},
                    "object": {"text": "10 bar", "start": 25, "end": 31},
                },
                "constraints": [],
                "references": [],
            }
        ],
    }

    record = Record.model_validate(payload)
    assert record.idx == 7
    assert record.original_text == "The valve shall withstand 10 bar."
    assert len(record.requirements) == 1
    assert record.requirements[0].structure.modality == "shall"


def test_record_manual_construction() -> None:
    record = Record(
        idx=1,
        original_text="The motor shall rotate.",
        requirements=[
            IndividualRequirement(
                req_idx=0,
                raw_text="The motor shall rotate.",
                structure=Structure(
                    subject=Span(text="The motor", start=0, end=9),
                    modality="shall",
                    condition=Condition(),
                    action=Span(text="shall rotate", start=10, end=22),
                    object=Span(text="", start=0, end=0),
                ),
            )
        ],
    )
    assert record.requirements[0].structure.action.text == "shall rotate"
