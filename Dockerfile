FROM python:3.9
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./name_screening_monolithic.py ./name_screening_monolithic.py
CMD ["python3","./name_screening_monoithic.py"]
