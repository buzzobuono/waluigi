# waluigi-developer

Claude Code skill for developing Waluigi data pipelines.

## What it does

Gives Claude operational mastery of the Waluigi orchestrator: designing jobs and tasks in YAML, testing locally with `wlrun`, deploying to a cluster with `wlctl`, and implementing the full Bronze→Silver→Gold medallion architecture with built-in tasks, Catalog integration, and data quality checks.

## Install

```bash
/plugin marketplace add buzzobuono/waluigi
/plugin install waluigi-developer@waluigi-marketplace
```

## Usage

Invoke explicitly:

```
/waluigi-developer
```

Or just describe your pipeline task — Claude activates the skill automatically when working on Waluigi pipelines.

## Skills

| Command | Description |
|---------|-------------|
| `/waluigi-developer` | Full Waluigi pipeline development guidance |
