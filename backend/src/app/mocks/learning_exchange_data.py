"""Only fixtures and provisional simulator settings belong here."""

from pathlib import Path

from pydantic import BaseModel, ConfigDict

from app.schemas.akajob.responses import AkajobCertificate
from app.schemas.fhu.responses import FHUEmployee
from app.schemas.skillup.certificates import SkillMapping

SOURCE_FILE = Path(__file__).with_name("fixtures") / "learning_sources.json"
DEMO_API_KEY = "local-learning-exchange-demo-only"
DEMO_MARKER = "fsa-learning-exchange-mock-v1"

DEMO_SKILLS: dict[str, SkillMapping] = {
    "python": {"taxonomySkillId": 97915, "skillId": 93285, "skillName": "Python"},
    "sql": {"taxonomySkillId": 97916, "skillId": 93286, "skillName": "SQL"},
}


class DemoSources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    employees: list[FHUEmployee]
    certificates: list[AkajobCertificate]
