FROM python:3.10

WORKDIR /pricer

# Copy the requirements to container
COPY requirements.txt requirements.txt

# Install requirements
RUN pip install --upgrade pip && pip install --no-cache-dir --upgrade -r requirements.txt

# Copy the source code to the container
COPY . /pricer

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]