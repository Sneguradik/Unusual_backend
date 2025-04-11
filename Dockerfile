# Use official Python image as base
FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install dependencies first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . .

# Expose the port Quart will run on
EXPOSE 8000

# Command to run the application
CMD ["hypercorn", "--bind", "0.0.0.0:8000", "app:app"]