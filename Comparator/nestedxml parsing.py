import xml.etree.ElementTree as et
import pandas as pd

tree = et.parse(r'all_aglu_emissions.xml')
root = tree.getroot()

lst1 = []

for x in root.iter('region'):
    root1=x
    for supply in root1.iter('AgSupplySector'):
        root2=(supply)
        for tech in root2.iter('AgProductionTechnology'):
            root3=(tech)
            for yr in root3.iter('period'):
                root4=yr
                for gas in root4.iter('Non-CO2'):
                    root5=gas
                    for em in root5.iter('input-emissions'):
                        lst1.append({'Technology': tech.attrib.get('name'), 'Value': em.text, 'Gas': gas.attrib.get('name'),
                                    'Year': yr.attrib.get('year'), 'Crop': supply.attrib.get('name'),
                                    'Country': x.attrib.get('name')})

print(lst1)
print('Creating dataframe')

test_df = pd.DataFrame(lst1)
print(test_df.head())