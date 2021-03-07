import requests
import py7zlib

def download_archieve():
    
    url2019 = 'https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2019.7z'
    r = requests.get(url2019, allow_redirects=True)
    with open('ZNO2019.7z', 'wb') as A19:
        A19.write(r.content)

    url2020 = 'https://zno.testportal.com.ua/yearstat/uploads/OpenDataZNO2020.7z'
    r = requests.get(url2020, allow_redirects=True)
    with open('ZNO2020.7z', 'wb') as A20:
        A20.write(r.content)

def extract_files_from_archieve():
    with open('ZNO2019.7z', 'rb') as A19:
        extract(py7zlib.Archive7z(A19),'ZNO2019.csv')
    print('ZNO2019.csv have been extrected')
    with open('ZNO2020.7z', 'rb') as A20:
        extract(py7zlib.Archive7z(A20),'ZNO2020.csv')
    print('ZNO2020.csv have been extrected')
    
def extract(Archive7z,outfilename):
    for name in Archive7z.getnames():
        if name.find('.csv') > 0:
            outfile = open(outfilename, 'wb')
            outfile.write(Archive7z.getmember(name).read())
            outfile.close()