import hashlib as h
import hmac
import base64 as b64
import requests


domain_name = "cdb.api.qcloud.com"
url = "https://{domain_name}/v2/index.php".format(domain_name = domain_name)

payload_general = {
    "Action": "GetCdbExportLogUrl",
    "Region": "",
    "Timestamp": "",
    "Nonce": "",
    "SecretId": "",
    "Signature": ""
}

session1 = requests.session()

request1 = session1.post(url)

def sign_string(source_string, signature_string):
    '''
    :param source_string: String to be signed
    :param signature_string: String used as signature
    :return: Signed string
    '''
    return b64.b64encode(hmac.new(signature_string.encode(), source_string.encode(), h.sha1).digest())

q = "GETcvm.api.qcloud.com/v2/index.php?Action=DescribeInstances&Nonce=11886&Region=gz&SecretId=AKIDz8krbsJ5yKBZQpn74WFkmLPx3gnPhESA&Timestamp=1465185768&instanceIds.0=ins-09dx96dg&limit=20&offset=0"
k = 'Gu5t9xGARNpq86cd98joQYCN3Cozk1qA'
b64.b64encode(
    hmac.new(k.encode(), q.encode(), h.sha1).digest()
    )

h.pbkdf2_hmac("sha1", q.encode("utf-8"), k.encode(), 12)
hmac.new(q.encode(), k.encode(), h.sha1)

'''
$key = "0f2100e34b54d50fd0138f300d3497579dae5279";
$get_data = "secret-message";
$decodedKey = pack("H*", $key);
$hmac = hash_hmac("sha1", $get_data, $decodedKey, TRUE);
$signature = base64_encode($hmac);
echo "key = $key\n";
echo "get_data = $get_data\n";
echo "signature = $signature";
'''

# key = "0f2100e34b54d50fd0138f300d3497579dae5279"
# get_data = "secret-message"
# decodedKey = key.decode("hex")
# hmac = hmac.new(decodedKey, get_data.encode('UTF-8'), hashlib.sha1)
# signature = hmac.digest().encode('base64')
# print "key =", key
# print "get_data =", get_data
# print "signature =", signature
#
# 'ripemd160', 'The quick brown fox jumped over the lazy dog.', 'secret'
