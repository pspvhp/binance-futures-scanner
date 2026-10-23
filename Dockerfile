FROM python:3.11-slim
WORKDIR /app
COPY . .
ENV PORT=5000
ENV HOST=0.0.0.0
EXPOSE 5000
CMD ["python", "scanner.py"]
