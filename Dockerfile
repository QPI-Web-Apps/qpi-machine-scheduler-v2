FROM python:3.12-slim

WORKDIR /app

# Prisma Client Python needs the prisma CLI (Node-based) to fetch the
# query engine binary, plus the OpenSSL runtime that the engine links
# against. ca-certificates is needed for the binary download over HTTPS.
RUN apt-get update && apt-get install -y --no-install-recommends \
        nodejs \
        npm \
        openssl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Schema must be present BEFORE `prisma generate`. We don't run db pull or
# any other DB-touching command at build time — generate is purely local
# code generation from the checked-in schema, no DATABASE_URL needed.
COPY prisma/ prisma/
RUN python -m prisma generate --schema=prisma/schema.prisma

COPY src/ src/
COPY frontend/ frontend/

EXPOSE 8000

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "8000"]
