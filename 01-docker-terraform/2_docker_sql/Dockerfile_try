# docker build  -t python:pandas .
# docker run  -it python:pandas 
FROM python:3.10

RUN pip install pandas

WORKDIR /app
COPY pypeline.py pypeline.py

# ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python" , "pypeline.py" ]