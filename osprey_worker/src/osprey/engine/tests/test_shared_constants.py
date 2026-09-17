from osprey.engine.shared_constants import OspreyEntityTypes


def test_provisional_account_entity_type_value() -> None:
    assert OspreyEntityTypes.PROVISIONAL_ACCOUNT == 'ProvisionalAccount'
