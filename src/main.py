from __future__ import annotations
from typing import Optional

import asyncio
import asyncio.base_subprocess as _bsp
import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path

_del = _bsp.BaseSubprocessTransport.__del__

def _patched_del(self):
    try:
        _del(self)
    except RuntimeError:
        pass

_bsp.BaseSubprocessTransport.__del__ = _patched_del

import click
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.config import get_settings
from src.config import setup_logging, get_logger
from src.storage.mongo_client import MongoClient
from src.storage.collections import ensure_indexes

logger = get_logger(__name__)


def create_app() -> FastAPI:
    from src.registry.router import router as registry_router, set_mongo_client

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.mongo_client = MongoClient()
        try:
            await app.state.mongo_client.connect()
        except Exception as exc:
            logger.critical("Failed to connect to MongoDB during startup", error=str(exc))
            raise exc

        try:
            set_mongo_client(app.state.mongo_client)
            db = app.state.mongo_client.get_database()
            await ensure_indexes(db)
        except Exception as exc:
            logger.critical("Failed to ensure indexes during startup", error=str(exc))
            await app.state.mongo_client.disconnect()
            raise exc

        logger.info("API server running")
        yield
        if app.state.mongo_client:
            try:
                await app.state.mongo_client.disconnect()
            except Exception as exc:
                logger.error("Error during MongoDB disconnect in shutdown", error=str(exc))
        logger.info("API server stopped")

    app = FastAPI(
        title="Dynamic Web Scraper",
        description="Web scraping system with plugin-based adapters",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(registry_router)

    @app.get("/health")
    async def health_check():
        return {"status": "healthy", "version": "1.0.0"}

    return app


@click.group()
def cli():
    setup_logging()


@cli.command()
@click.option("--host", default=None, help="API host")
@click.option("--port", default=None, type=int, help="API port")
def serve(host: Optional[str], port: Optional[int]):
    try:
        settings = get_settings()
    except Exception as exc:
        click.echo(f"Configuration error (invalid .env or settings): {exc}", err=True)
        sys.exit(1)

    uvicorn.run(
        "src.main:create_app",
        factory=True,
        host=host or settings.api_host,
        port=port or settings.api_port,
        reload=False,
        log_level=settings.log_level.lower(),
    )


@cli.command()
@click.option(
    "--duration",
    default=None,
    type=int,
    help="Pipeline duration in seconds (default: run indefinitely)",
)
@click.option(
    "--output",
    default=None,
    type=str,
    help="Output JSON file path for captured events",
)
def run(duration: Optional[int], output: Optional[str]):
    try:
        settings = get_settings()
    except Exception as exc:
        click.echo(f"Configuration error (invalid .env or settings): {exc}", err=True)
        sys.exit(1)

    if output is None:
        output = str(Path(settings.scrape_output_dir) / "pipeline_run.json")

    try:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        click.echo(f"Error: Could not create output directory structure: {exc}", err=True)
        sys.exit(1)

    async def _run():
        mongo = MongoClient()
        try:
            await mongo.connect()
        except Exception as exc:
            click.echo(f"Database Connection Error: Could not connect to MongoDB. Details: {exc}", err=True)
            sys.exit(1)

        db = mongo.get_database()
        try:
            await ensure_indexes(db)
        except Exception as exc:
            click.echo(f"Database Indexing Error: Failed to setup indexes: {exc}", err=True)
            await mongo.disconnect()
            sys.exit(1)

        from src.engine.orchestrator import ScrapingOrchestrator

        orchestrator = ScrapingOrchestrator(mongo, settings)

        try:
            await orchestrator.start(
                duration_seconds=duration,
                output_file=output,
            )
        except KeyboardInterrupt:
            logger.info("Pipeline stopped by user")
        except Exception as exc:
            logger.error("Pipeline run error", error=str(exc), exc_info=True)
            click.echo(f"Pipeline error: {exc}", err=True)
        finally:
            await mongo.disconnect()

    asyncio.run(_run())


@cli.command()
@click.option(
    "--file",
    default="seeds/sources.json",
    help="Path to seed data JSON file",
)
def seed(file: str):
    try:
        settings = get_settings()
    except Exception as exc:
        click.echo(f"Configuration error (invalid .env or settings): {exc}", err=True)
        sys.exit(1)

    async def _seed():
        seed_path = Path(file)
        if not seed_path.exists():
            click.echo(f"Error: Seed file not found: {file}", err=True)
            sys.exit(1)

        try:
            with open(seed_path) as f:
                sources = json.load(f)
        except json.JSONDecodeError as exc:
            click.echo(f"Error: Failed to parse seed JSON file '{file}': {exc}", err=True)
            sys.exit(1)
        except Exception as exc:
            click.echo(f"Error: Failed to read seed file '{file}': {exc}", err=True)
            sys.exit(1)

        mongo = MongoClient()
        try:
            await mongo.connect()
        except Exception as exc:
            click.echo(f"Database Connection Error: Could not connect to MongoDB. Details: {exc}", err=True)
            sys.exit(1)

        db = mongo.get_database()
        try:
            await ensure_indexes(db)
        except Exception as exc:
            click.echo(f"Database Indexing Error: Failed to setup indexes: {exc}", err=True)
            await mongo.disconnect()
            sys.exit(1)

        from src.registry.models import SourceConfig, SourceConfigUpdate
        from src.registry.repository import SourceRepository

        repo = SourceRepository(db)

        seeded = 0
        updated = 0
        for source_data in sources:
            try:
                source = SourceConfig(**source_data)
                existing = await repo.get_source(source.source_id)
                if existing:
                    update = SourceConfigUpdate(**source_data)
                    await repo.update_source(source.source_id, update)
                    updated += 1
                    click.echo(f"  Updated: {source.source_id}")
                else:
                    await repo.create_source(source)
                    seeded += 1
                    click.echo(f"  Created: {source.source_id}")
            except Exception as exc:
                click.echo(f"  Failed to seed source data {source_data.get('source_id', 'unknown')}: {exc}", err=True)

        await mongo.disconnect()
        click.echo(f"\nSeeding complete: {seeded} created, {updated} updated")

    asyncio.run(_seed())


app = create_app()


if __name__ == "__main__":
    cli()
