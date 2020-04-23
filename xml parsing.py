import xml.etree.ElementTree as et
import pandas as pd

tree = et.parse("data.xml")
root = tree.getroot()

rows = []

for node in root:
    s_name = node.attrib.get('name')
    s_email = node.find('email').text if node is not None else None
    s_grade = node.find('grade').text if node is not None else None
    s_age = node.find('age').text if node is not None else None
    rows.append({'name':s_name,'email':s_email,'grade':s_grade,'age':s_age})

df = pd.DataFrame(rows)
print(df)
