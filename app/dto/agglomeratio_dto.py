from pydantic import BaseModel, Field


class RegionAgglomerationDTO(BaseModel):

    region_id: int = Field(examples=[1], title="Region ID")
    time: int = Field(
        default=80, ge=50, examples=[80], description="Agglomeration time in minutes"
    )
