from unifysql.ingestion.enricher import MetadataEnricher

enricher = MetadataEnricher(schema=[], engine=None)


def test_infer_fk() -> None:
    """Pytest unit test for the function `_infer_fk(column_name)`."""
    assert enricher._infer_fk("patient_id")
    assert not enricher._infer_fk("avg_revenue")
    assert enricher._infer_fk("USER_ID")
    assert not enricher._infer_fk("id")
    assert not enricher._infer_fk("id_number")
