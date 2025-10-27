from pydantic import BaseModel, Field, field_validator

from app.common.validators.region_validators import validate_region


class RegionAgglomerationDTO(BaseModel):

    region_id: int = Field(examples=[1], title="Region ID")
    time: int = Field(
        default=80, ge=50, examples=[80], description="Agglomeration time in minutes"
    )

    @field_validator("region_id")
    @classmethod
    def validate_region_id(cls, v):
        return validate_region(v)
