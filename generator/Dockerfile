FROM python:3.8-slim

WORKDIR /app

# Copy app handler code
COPY ./generator.py .

# copy requirements.txt
COPY ./requirements.txt .

# install dependencies no cache
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "python", "generator.py"]
