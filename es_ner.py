'''
python V3.8+

1) Spacy
pip install -U pip setuptools wheel
pip install -U spacy
python -m spacy download en_core_web_sm

or

conda install -c conda-forge spacy
python -m spacy download en_core_web_sm

2) ElasticSearch
python -m pip install elasticsearch

'''
import spacy
import uuid
import datetime;
from html.parser import HTMLParser
from elasticsearch import Elasticsearch 

def log(message):
    """
    Custom log function to insert timestamp in message
    """
    ct = datetime.datetime.now()
    print(f"[{ct}] {message}")

class MyHTMLParser(HTMLParser):
    """
    Parser to grab text only from html: 
        - Collect text to be fed to NER pipe 
        - Replace text with temporary identifier 
        
    data - Entire html string 
    nerIdentifierList - List of temporary identifier inserted into data 
    nerDataList - List of text extracted, which will be fed to NER later 
    identifierCount - Index of temporary identifier 
    identifierKey - Gurantee uniqueness of temporary identifier
    """
    
    def __init__(self):
        super().__init__()
        self.data = ""
        self.nerIdentifierList = []
        self.nerDataList = []
        self.identifierCount = 0
        self.identifierKey = uuid.uuid4()
        
    def handle_starttag(self, tag, attrs):
        if (tag == 'ner'):
            return
        attributes = ""
        for attr in attrs:
            attrVal = attr[1]
            if not attrVal:
                attrVal = ''
                continue
            if not isfloat(attrVal): #add quote if not numeric
                attrVal = f"'{attrVal}'"
            attributes = f"{attr[0]}={attrVal} "
        self.data += f'<{tag} {attributes.strip()}>'
    def handle_endtag(self, tag):
        if (tag == 'ner'):
            return
        self.data += f'</{tag}>'

    def handle_data(self, data):
        if (data and not data.isspace()):
            #self.data += nerProcess(data)
            identifier = f"[{self.identifierKey}-{self.identifierCount}]"
            self.data += identifier
            self.nerIdentifierList.append(identifier)
            self.nerDataList.append(data)
            self.identifierCount += 1
        else:
            self.data += data

def isfloat(value):
    """
    Check if string is float 
    Return True or False
    """
    try:
        float(value)
        return True
    except ValueError:
        return False   

def recursiveLook(data, isLast):
    """
    Tranverse sma_data_json:
        - Collect actual data if NER result does not exist
        - Replace actual data with NER result if exist 
    Return entire data
    """
    obj = data.get('obj')
    if (type(obj) is dict):
        len(obj)
        for key in obj: # Recursive tranverse dict to find string
            isLast = key == list(obj.keys())[-1]
            if key == 'mmmsss':
                isLast = False
            data['obj'] = obj.get(key)
            result = recursiveLook(data, isLast)
            obj[key] = result['obj']
            data['obj'] = obj
    elif (type(obj) is list): # Recursive tranverse list to find string
        for i in range(len(obj)):
            isLast = (i == len(obj) - 1)
            data['obj'] = obj[i]
            result = recursiveLook(data, isLast)
            obj[i] = result['obj']
            data['obj'] = obj
    else:
        if isLast: # Recursive found string
            if 'nerResultList' in data:
                # If NER is done, replace data with NER data
                nerResultList = data.get('nerResultList')
                for i in range(len(data.get('nerIdentifierList'))):
                    identifier = data.get('nerIdentifierList')[i]
                    result = nerResultList[i]
                    obj = obj.replace(identifier, result)
                data['obj'] = obj
            else:
                # If NER is not performed yet, collect all data
                parser = MyHTMLParser()
                parser.feed(obj)
                
                data['obj'] = parser.data
                data['nerIdentifierList'] = data.get('nerIdentifierList') + parser.nerIdentifierList
                data['nerDataList'] = data.get('nerDataList') + parser.nerDataList
                
                parser.close()  
    return data

def nerProcessPipeLine(textList):
    """
    Perform ner on list of string 
    Return list of string result with ner tag added
    """
    nlp = spacy.load("en_core_web_sm")
    nlp.select_pipes(enable="ner")
    
    nerResults = []
    for doc in nlp.pipe(textList):
        wordList = []
        for token in doc:
            tokenText = token.text
            if token.whitespace_:
                tokenText += token.whitespace_
            wordList.append(tokenText)
            
        for ent in doc.ents:
            if not ent.label_ == 'ORG':
                continue
            
            wordList[ent.start] = f"<ner type='{ent.label_}'>" + ent.text  + '</ner> '
            for i in range(ent.start + 1, ent.end):
                wordList[i] = ''
                
        resultText = ""
        for obj in wordList:
            resultText += obj
        
        nerResults.append(resultText)
    
    return nerResults
        
def es_iterate_all_documents(es, index, pagesize=250, **kwargs):
    """
    Helper to iterate ALL values from
    Yields all the documents.
    """
    offset = 0
    while True:
        result = es.search(index=index, **kwargs, size=pagesize, from_=offset)
        hits = result["hits"]["hits"]
        # Stop after no more docs
        if not hits:
            break
        # Yield each entry
        yield from (hit for hit in hits)
        # Continue from there
        offset += pagesize

def processEsDoc(es, esDoc):
    filing = esDoc['_source']
    data = {
        'obj': filing.get('sma_data_json'),
        'nerIdentifierList': [],
        'nerDataList': []
    }

    # walk through to collect text
    log("Performing collection")
    data = recursiveLook(data, False)
    
    # perform ner
    log("Performing NER")
    data['nerResultList'] = nerProcessPipeLine(data['nerDataList'])
    
    # replace data with ner result
    log("Replacing data with NER data")
    data = recursiveLook(data, False)
    
    #save results
    log("Save to index")
    filing['sma_data_json'] = data.get('obj')
    es.index(index="filing_enr", id=esDoc['_id'], document=filing)

def main():
    es = Elasticsearch(
        ['localhost'],
        http_auth=('USER', 'PASSWORD'),
        scheme="http",
        port=PORT
    )   
    
    # esDoc = es.get(index="TARGET_INDEX", id='DOC_ID')
    # processEsDoc(es, esDoc)
    
    count = 0;
    for esDoc in es_iterate_all_documents(es, 'TARGET_INDEX'):        
        log(f"[START] {esDoc['_id']}")
        processEsDoc(es, esDoc)
        count += 1        
        log(f"[END] {count} {esDoc['_id']}")
    
main()