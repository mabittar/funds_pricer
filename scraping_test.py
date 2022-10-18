import httpx


CVM_URL = "https://cvmweb.cvm.gov.br/SWB/Sistemas/SCW/CPublica/CConsolFdo/ResultBuscaParticFdo.aspx?CNPJNome=24.078.037/0001-09&TpPartic=0&Adm=false&SemFrame="

        
        
def main(document):
    with httpx.Client() as client:
        
        fund = client.get(CVM_URL + document)
        
        print(fund)
        
    """
    name = span id="lbNmDenomSocial
    first_date = span id="lbInfAdc2"
    """
    

if __name__ == '__main__':
    try:
       main("24.078.037/0001-09")
    
    except Exception as e:
       print(e)