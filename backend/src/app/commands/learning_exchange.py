"""Local-only composition of the shared certificate exchange service."""

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any

import httpx
from pydantic import SecretStr

from app.clients.akajob_client import AkajobClient
from app.clients.certificate_exchange_http import CertificateExchangeError, exchange_request
from app.clients.fhu_client import FHUClient
from app.clients.skillup_certificate_client import SkillUpCertificateClient
from app.mocks.learning_exchange_data import DEMO_API_KEY, DEMO_MARKER, DEMO_SKILLS
from app.repositories.local_writer import LocalBronzeWriter
from app.services.skillup.certificate_exchange import run_certificate_exchange


def validate_demo_url(base_url: str) -> str:
    url = httpx.URL(base_url)
    if (
        url.scheme != "http"
        or url.host not in {"127.0.0.1", "::1"}
        or url.path != "/"
        or url.query
        or url.fragment
        or url.userinfo
    ):
        raise CertificateExchangeError("Demo requires a loopback HTTP origin without credentials")
    return str(url).rstrip("/")


async def run_local_exchange(
    client: httpx.AsyncClient,
    base_url: str,
    output_dir: Path,
    *,
    max_polls: int = 20,
    poll_interval: float = 0.1,
) -> dict[str, Any]:
    base_url = validate_demo_url(base_url)
    marker = await exchange_request(client, "GET", base_url + "/demo/info")
    if marker.json() != {"service": DEMO_MARKER}:
        raise CertificateExchangeError("Target is not the dedicated learning exchange mock")
    # Fixed mock-only credential, not production environment variables.
    client.headers["x-api-key"] = DEMO_API_KEY
    output_dir = await asyncio.to_thread(output_dir.resolve)
    result = await run_certificate_exchange(
        FHUClient(client, base_url + "/fhu"),
        AkajobClient(client, base_url + "/akajob"),
        SkillUpCertificateClient(client, base_url + "/skillup", SecretStr(DEMO_API_KEY)),
        LocalBronzeWriter(output_dir),
        DEMO_SKILLS,
        max_polls=max_polls,
        poll_interval=poll_interval,
    )
    return {**result, "mode": "mock-only", "output_dir": str(output_dir)}


async def _main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:9100")
    parser.add_argument("--output-dir", type=Path, default=Path("data/learning-exchange-demo"))
    parser.add_argument("--max-polls", type=int, default=20)
    parser.add_argument("--poll-interval", type=float, default=0.1)
    args = parser.parse_args()
    async with httpx.AsyncClient(trust_env=False) as client:
        try:
            result = await run_local_exchange(
                client,
                args.base_url,
                args.output_dir,
                max_polls=args.max_polls,
                poll_interval=args.poll_interval,
            )
        except CertificateExchangeError as exc:
            raise SystemExit(str(exc)) from None
        except (ValueError, KeyError, TypeError):
            raise SystemExit("Exchange contract validation failed; no payload logged") from None
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
