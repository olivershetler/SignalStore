FROM registry.gitlab.com/hussainilab1/neuroscikit-backend-base:latest

WORKDIR /code

COPY ./pyproject.toml ./
COPY ./src ./src

RUN pip install -e .