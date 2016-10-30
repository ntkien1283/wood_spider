import scrapy
import ipdb
import _mysql
import numpy
class SupplierInfo:
    def __init__(self):
        self.name = ''
        self.address = ''
        self.phone  = ''
        self.email = ''
        self.intro  = ''
class SupplierSpider(scrapy.Spider):
    name = 'supplier'
    def start_requests(self):
        source_url = 'http://yellowpages.vnn.vn/cls/262110/go-nguyen-lieu.html'
        urls = [source_url]
#        for i in numpy.arange(2, 11):
#            urls.append(source_url + '?page=%d'%i)
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)
    def get_row_count(self, table, db_conn):
        db_conn.query('select count(*) from ' + table)
        result = db_conn.use_result()
        result = result.fetch_row()
        return int(result[0][0])
    def insert_into_supplier(self, db_conn, supplier_info, verbose = False):
        # sql  to store unicode: alter table Supplier  Engine=InnoDB checksum=1 comment='' delay_key_write=1 row_format=dynamic charset=utf8 collate=utf8_unicode_ci 
        supplier_id = self.get_row_count('Supplier', db_conn) + 1
        sql_insert = """insert into Supplier (id, name, address, phone, intro) values ({:d}, '{:s}', '{:s}', '{:s}', '{:s}')""".format(supplier_id, supplier_info.name, supplier_info.address, supplier_info.phone,  supplier_info.intro) 
        db_conn.query(sql_insert)
        if verbose:
            db_conn.query('Select * from Supplier')
            results = db_conn.use_result()
            cont = True
            while cont:
                row = results.fetch_row()
                if row is not None:
                    print (row)
                else:
                    cont = False
    def parse_detail_page(self, page):
        return ''
    def parse(self, response):
        #detail_pages = response.xpath('//a[re:test(@class, "buttonMoreDetails")]//@href').extract()
        detail_pages = response.xpath('//a[contains(@class, "bottonMoreDetails")]//@href') 
        supplier_info = SupplierInfo()
        names_div = response.css('div.listing_head')
        other_info_div = response.css('div.company_content')
        db_conn = _mysql.connect('localhost', 'root', 'Fibo01', 'wood_industry')
        db_conn.query('use wood_industry;')
        for  i in range(len(names_div)):
            supplier_info.name = names_div[i].css('h2.company_name').css('a::text').extract_first().encode('utf8')
            self.parse_detail_page(detail_pages[i])
            text = other_info_div[i].css('div.listing_logo_dc').css('p.listing_diachi::text').extract_first()
            if text:
                supplier_info.address = text.encode('utf8')

            text = other_info_div[i].css('div.listing_logo_dc').css('p.listing_tel::text').extract_first()
            if text:
                supplier_info.phone = text.encode('utf8')

            text = other_info_div[i].css('div.listing_logo_dc').css('p.listing_email').css('a::text').extract_first()
            if text:
                supplier_info.email = text.encode('utf8')

            text = other_info_div[i].css('div.listing_textqc').css('p::text').extract_first()
            if text: 
                supplier_info.intro = text.encode('utf8')

            self.insert_into_supplier(db_conn, supplier_info) 
