from __future__ import annotations

from pathlib import Path
from typing import Literal
import subprocess
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field, field_validator

from app.state import AnnotationManager

BASE_DIR = Path(__file__).parent.resolve()
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
CHARTS_DIR = BASE_DIR / "charts"

TEMPLATES_DIR.mkdir(exist_ok=True)
STATIC_DIR.mkdir(exist_ok=True)
CHARTS_DIR.mkdir(exist_ok=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

state_manager = AnnotationManager(csv_dir=BASE_DIR / "csv", chart_dir=CHARTS_DIR, events_file=BASE_DIR / "marked" / "events.json")


class EventPayload(BaseModel):
    event_type: Literal["overflow", "lost"] = Field(description="事件类型")
    start_file: str = Field(description="起始文件")
    start_row: int = Field(gt=0, description="起始行号")
    end_file: str = Field(description="结束文件")
    end_row: int = Field(gt=0, description="结束行号")

    @field_validator("start_file", "end_file")
    @classmethod
    def _strip(cls, value: str) -> str:
        return value.strip()


class EventUpdatePayload(BaseModel):
    event_type: Literal["overflow", "lost"] = Field(description="事件类型")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 检查综合指标图表是否存在
    comprehensive_dir = CHARTS_DIR / "综合指标"
    if not comprehensive_dir.exists() or not any(comprehensive_dir.glob("*.png")):
        print("综合指标图表不存在，正在生成...")
        try:
            # 运行 to_charts.py 生成图表
            result = subprocess.run(
                ["uv", "run", "to_charts.py"],
                cwd=BASE_DIR,
                capture_output=True,
                text=True,
                check=True
            )
            print("图表生成完成")
        except subprocess.CalledProcessError as e:
            print(f"图表生成失败: {e.stderr}")
            raise
    else:
        print("综合指标图表已存在，跳过生成")

    state_manager.initialize()
    yield

app = FastAPI(title="Overflow Annotation Service", version="1.0", lifespan=lifespan)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
app.mount("/charts", StaticFiles(directory=CHARTS_DIR), name="charts")


@app.get("/", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
        },
    )


@app.get("/api/files")
async def list_files() -> dict:
    files = state_manager.list_files()
    return {"files": files}


@app.get("/api/events")
async def list_events() -> dict:
    events = state_manager.list_events()
    return {"events": events}


@app.post("/api/events", status_code=201)
async def create_event(payload: EventPayload) -> dict:
    try:
        event = state_manager.add_event(
            event_type=payload.event_type,
            start_file=payload.start_file,
            start_row=payload.start_row,
            end_file=payload.end_file,
            end_row=payload.end_row,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"event": event.to_dict()}


@app.delete("/api/events/{event_id}", status_code=204)
async def delete_event(event_id: str) -> None:
    try:
        state_manager.delete_event(event_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="事件不存在") from exc


@app.patch("/api/events/{event_id}")
async def update_event(event_id: str, payload: EventUpdatePayload) -> dict:
    try:
        event = state_manager.update_event_type(event_id, payload.event_type)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail="事件不存在") from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    return {"event": event.to_dict()}


@app.post("/api/export")
async def export_annotations() -> dict:
    exported = state_manager.export_marked_data()
    return {"exported": exported}
