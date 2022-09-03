FROM python:3.9.12
ENV CH_HOST=localhost
COPY . .
RUN pip install -r req.txt
CMD [ "uvicorn", "main:app","--host", "0.0.0.0","--reload" ]

