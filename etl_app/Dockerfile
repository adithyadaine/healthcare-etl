# Use an official Python 3.9 image.
FROM python:3.9-slim

# Set the working directory inside the container. All subsequent commands run here.
WORKDIR /app

# Copy the requirements file into the container.
COPY requirements.txt .

# Install the Python libraries listed in requirements.txt.
RUN pip install --no-cache-dir -r requirements.txt

# Copy the main ETL script into the container.
COPY etl_script.py .

# The command that will be executed when the container starts.
CMD ["python", "etl_script.py"]