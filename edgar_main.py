# Generate the list of index files archived in EDGAR since start_year (earliest: 1994) until the most recent quarter
import sqlite3
import csv
import pandas
from concurrent.futures import ProcessPoolExecutor, as_completed
import requests
import os
import html2text
import concurrent.futures


# database functions copied from found github project
# pulls all archived Edgar filings
def make_idx(file_name):
    start_year = 1994       # change start_year and end_year to re-define the chunk
    current_year = 2019     # change start_year and end_year to re-define the chunk
    current_quarter = 4     # do not change this line
    
    
    years = list(range(start_year, current_year))
    quarters = ['QTR1', 'QTR2', 'QTR3', 'QTR4']
    history = [(y, q) for y in years for q in quarters]
    for i in range(1, current_quarter + 1):
        history.append((current_year, 'QTR%d' % i))
    urls = ['https://www.sec.gov/Archives/edgar/full-index/%d/%s/master.idx' % (x[0], x[1]) for x in history]
    urls.sort()
        
    db_filename = file_name.replace('.csv', '.db')
    con = sqlite3.connect(db_filename)
    cur = con.cursor()
    cur.execute('DROP TABLE IF EXISTS idx')
    cur.execute('CREATE TABLE idx (cik TEXT, conm TEXT, type TEXT, date TEXT, path TEXT)')
    
    for url in urls:
        lines = requests.get(url).content.decode("utf-8", "ignore").splitlines()
        records = [tuple(line.split('|')) for line in lines[11:]]
        cur.executemany('INSERT INTO idx VALUES (?, ?, ?, ?, ?)', records)
        print(url, 'downloaded and wrote to SQLite')
    
    con.commit()
    con.close()


# output database to csv for my own comfort in working with data
# creates master spreadsheet of Edgar filings
def db_to_csv(file_name):
    print('Translating Database to CSV')
    db_filename = file_name.replace('.csv', '.db')
    conn = sqlite3.connect(db_filename)
    curs = conn.cursor()
    query = 'select * from idx'
    curs.execute(query)
    
    results = pandas.read_sql_query(query, conn)
    results.to_csv(file_name, index=False)
        
# get text files containing urls to pull from the spreadsheet of Edgar pages
def csv_to_texts(csv_filename, forms):
    print('Compiling Lists of URLs From Each Form-Type to Scrape')
    root_url = 'https://www.sec.gov/Archives/'
    lines = []
    with open(csv_filename, 'r') as fin:
        reader = csv.reader(fin)
        for row in reader:
            lines.append(row)
    
    for line in lines:
        for form in forms:
            if form == line[2].lower():
                txt_file_name = form + '.txt'
                if '/' in txt_file_name:
                    txt_file_name = txt_file_name.replace('/a', '')
                with open(txt_file_name, 'a') as fout:
                    fout.write(root_url + line[-1] + '\n')
            
        
    

    

# pull edgar text files from list of urls
def get_form_page(url):
    print('Processing: {}'.format(url.strip()))
    r = requests.get(url)
    r_text = r.text
    
    with open('test.txt', 'w') as fout:
        fout.write(r_text)
        
    with open('test.txt', 'r') as fin:
        lines = fin.readlines()
    os.remove('test.txt')

    page_text_file = 'test.txt'
    with open(page_text_file, 'w') as fout:
        fout.write('url: ' + url + '\n')
        for line in lines:
            if line.strip() == '':
                pass
            else:
                fout.write(line.strip().lower() + '\n')

    process_page(page_text_file, url)
    os.remove(page_text_file)


# function to pull all desired data/variables from an Edgar 'S' filing
def process_page(page_path, url):
    page_url = url
    company_name = ''
    cik = ''
    filing_date = ''
    form = ''

    # keywords to look for in page
    split_key_words = ['splitoff', 'split off', 'split-off']
    split_num_present = 0
    
    exchange_key_words = ['exchange offer', 'tax-free exchange', 'tax free exchange', 'share exchange', 'exchange of share', 'exchange-of-share']
    exchange_num_present = 0
    
    stock_exchange_words = ['stock exchange']
    stock_exchange_num_present = 0
    
    with open(page_path, 'r') as fin:
        page_lines = fin.readlines()
    
    # going through each line of page text looking for desired data/variables
    for line in page_lines:
        # if keywords in file take note and count up
        if any(word in line for word in split_key_words):
            split_num_present += 1
        
        if any(word in line for word in exchange_key_words):
            exchange_num_present += 1
        
        if any(word in line for word in stock_exchange_words):
            stock_exchange_num_present += 1

        # set page info
        if 'company conformed name:' in line.strip():
            company_name = line.split('company conformed name:')[1].strip()
            
        if 'central index key:' in line:
            cik = line.split('central index key:')[1].strip()
            
        if 'filed as of date:' in line:
            filing_date = line.split('filed as of date:')[1].strip()
            filing_date = filing_date[0:4] + '/' + filing_date[4:6] + '/' + filing_date[6:]
            
        if 'conformed submission type:' in line:
            form = line.split('conformed submission type:')[1].strip()

    #set output line for results spreadsheet
    out_line = [
        cik,
        filing_date,
        form,
        company_name,
        page_url,
        split_num_present,
        exchange_num_present,
        stock_exchange_num_present
    ]

    # output to results spreadsheet
    with open('compiled.csv', 'a') as fout:
        writer = csv.writer(fout)
        writer.writerow(out_line)
        
         
    
    
# set results spreadsheet header
def write_csv_header():
    print('Writing Ouput CSV Header')
    header = [
        'CIK', #Edgar Identifier
        'FDATE', #Filing Date
        'Form', #Type
        'CONAME', #Company Name
        'FNAME', #URL
        'Splitoff', #Number of times a splitoff keyword is mentioned in the document
        'Exchange', #Number of times an exchange keyword is mentioned in the document
        'Stock Exchange', #Number of times 'stock exchange' is mentioned in the document
        ]

    with open('compiled.csv', 'w') as fout:
        writer = csv.writer(fout)
        writer.writerow(header)

        
    
def main_setup():
    # list of the form types being looked at
    form_types = ['8-k', 'sc 13e4', 'sc to-i', '425', 'sc 13d', 'sc 13d/a']
    
    # filename for the master-spreadsheet of edgar urls for desired filetypes
    edgar_data = 'edgar_data.csv'
    # if the master spreadsheet doesn't exist we need to create it from Edgar directory data
    if not os.path.exists(edgar_data):
        make_idx(edgar_data)
        db_to_csv(edgar_data)
        
    print('Edgar Master CSV Available For Processing')
    db_filename = edgar_data.replace('.csv', '.db')
    if os.path.exists(db_filename):
        os.remove(db_filename)
    
    text_file_names = []
    for form in form_types:
        text_file_names.append(form + '.txt')
    
    csv_to_texts(edgar_data, form_types)
    
    
def process_form_urls():
    write_csv_header()
    file_names = [ '8-k.txt', 'sc 13d.txt', 'sc 13e4.txt', '425.txt', 'sc to-i.txt'] 
    for file_name in file_names:
        lines = []
        with open(file_name, 'r') as fin:
            lines = fin.readlines()
            #with concurrent.futures.ProcessPoolExecutor(max_workers=None) as executor:
            #    future_to_file = {executor.submit(get_form_page, line): line for line in lines}
            for line in lines:
                if line == '':
                    pass
                else:
                    get_form_page(line.strip())


def count_num_entries():
    file_names = [ '8-k.txt', 'sc 13d.txt', 'sc 13e4.txt', '425.txt', 'sc to-i.txt'] 
    for file_name in file_names:
        with open(file_name, 'r') as fin:
            good_lines = []
            lines = fin.readlines()
            for line in lines:
                if line == '':
                    pass
                else:
                    good_lines.append(line)
            
            print(file_name + ':' + str(len(good_lines)))
        
        print()
    

def combine_csvs():
    write_csv_header()
    csvs_folder = 'csvs'
    main_folder = os.getcwd()
    combined = os.path.join(main_folder, csvs_folder)
    csvs = os.listdir(combined)
    sheet_lines = []
    for csv_file in csvs:
        if csv_file == '.DS_Store':
            pass
        else:
            csv_filename = os.path.join(combined, csv_file)
            with open(csv_filename, 'r') as fin:
                reader = csv.reader(fin)
                for line in reader:
                    sheet_lines.append(line)
        
    
    
    with open('compiled.csv', 'a') as fout:
        writer = csv.writer(fout)
        for line in sheet_lines:
            writer.writerow(line)
   
        
    
#process_form_urls()
#count_num_entries()
combine_csvs()


    
            
    
    
    

    
    
    
    

            
    
    
    
    
    
    
    