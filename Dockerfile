FROM python:3.8

# /notebook folder will be created and set as working directory
WORKDIR /notebook
COPY requirements.txt .
# installing all dependancies packages in the container
RUN pip install -r requirements.txt