import requests
from datetime import datetime

url = 'http://139.199.31.200:13506/mildew/predict/normal'

begin = datetime.now()
hdr = open('data/111.hdr', "rb")
spe = open('data/111.spe', "rb")
test_response = requests.post(url, files={"hdr": hdr,"spe":spe}, data={"type":"hefei", "alpha":0.005})
end = datetime.now()
check_cost = end - begin
print("Check image cost {} ms".format(check_cost.microseconds / 1000))
print(test_response.text)

