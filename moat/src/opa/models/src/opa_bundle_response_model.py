from pydantic import BaseModel, ConfigDict, Field


class OpaBundleManifest(BaseModel):
    # OPA may include additional manifest fields (e.g. metadata); keep them.
    model_config = ConfigDict(extra="allow")

    revision: str = ""
    roots: list[str] = Field(default_factory=list)


class OpaBundleState(BaseModel):
    model_config = ConfigDict(extra="allow")

    etag: str = ""
    manifest: OpaBundleManifest = Field(default_factory=OpaBundleManifest)


class OpaBundleResponse(BaseModel):
    """
    Response from OPA's `data.system.bundles` document, where OPA records the
    current state of each downloaded bundle (manifest and etag), keyed by
    bundle name.
    """

    result: dict[str, OpaBundleState] = Field(default_factory=dict)
