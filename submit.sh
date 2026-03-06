#!/bin/bash

curl -X POST http://localhost:8082/submit -H "Content-Type: application/json" -d '{
        "module": "test2",
        "class": "GlobalReport",
        "params": {"date": "2026-03-05"}
    }'