.PHONY: up demo inspect test lint down clean
up:      ; docker compose up -d --build
demo:    ; docker compose run --rm pipeline python -m src.cli demo
inspect: ; docker compose run --rm pipeline python -m src.cli inspect
test:    ; docker compose run --rm pipeline pytest
lint:    ; ruff check .
down:    ; docker compose down
clean:   ; docker compose down -v   # DELETES all MinIO + catalog data
