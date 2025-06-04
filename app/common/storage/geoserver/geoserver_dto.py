from dataclasses import dataclass

from pydantic import BaseModel, Field


@dataclass()
class PopFrameGeoserverDTO:
    host: str
    workspace: str
    layer: str
    href: str


class PopFrameGeoserverData(BaseModel):
    host: str = Field(description="host:port for target instance")
    workspace: str = Field(description="datastore name")
    layer: str = Field(description="layer name")
    href: str = Field(description="href from initial response")

    @classmethod
    def from_dto(cls, dto: PopFrameGeoserverDTO):
        return cls(
            host=dto.host,
            workspace=dto.workspace,
            layer=dto.layer,
            href=dto.href,
        )
