FROM python:3.9-slim

WORKDIR /app

# System deps + Chrome + Xvfb
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg xvfb \
    fonts-liberation libnss3 libatk-bridge2.0-0 libatk1.0-0 \
    libx11-xcb1 libxcomposite1 libxdamage1 libxrandr2 libgbm1 \
    libgtk-3-0 libasound2 libxfixes3 libxkbcommon0 \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://dl.google.com/linux/linux_signing_key.pub | gpg --dearmor -o /usr/share/keyrings/google.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

COPY backend/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY backend /app
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99

CMD ["bash", "-lc", "Xvfb :99 -screen 0 1920x1080x24 & uvicorn app.main:app --host 0.0.0.0 --port 8000"]
