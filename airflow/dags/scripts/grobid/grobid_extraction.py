import os
import xml.etree.ElementTree as ET
import csv
import re
import datetime
import boto3
from urllib.parse import urlparse
from grobid_client_python.grobid_client.grobid_client import GrobidClient

class PDFProcessor:
    def __init__(self, s3_url):
        self.s3_url = s3_url
        self.input_directory = "pdf_inputs/"
        self.output_directory = "grobid_outputs/"
        self.xml_input_directory = "grobid_outputs/"
        self.csv_data = []

    def s3_download(self):
        parsed_url = urlparse(self.s3_url)
        bucket_name = parsed_url.netloc
        key = parsed_url.path.lstrip('/')

        s3_client = boto3.client('s3',
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"))

        try:
            contents = s3_client.get_object(Bucket=bucket_name, Key=key)['Body'].read()
            return contents
        except Exception as e:
            print(f"Error downloading from S3: {e}")
            return False

    def s3_upload(self, contents: bytes, key: str, folder: str):
        s3_client = boto3.client('s3',
            aws_access_key_id=os.getenv("aws_access_key_id"),
            aws_secret_access_key=os.getenv("aws_secret_access_key"))
        full_key = os.path.join(folder, key)
        s3_client.put_object(Bucket="file-storage-assignment-4", Key=full_key, Body=contents)

    def download(self):
        if not self.s3_url:
            return False
        contents = self.s3_download()
        file_name_ = os.path.basename(self.s3_url)
        with open(os.path.join(self.input_directory, file_name_), 'wb') as file:
            file.write(contents)

    def process_pdfs(self):
        client = GrobidClient(config_path="./config.json")
        client.process("processFulltextDocument", self.input_directory, self.output_directory, n=1, consolidate_header=False, consolidate_citations=True, force=True, segment_sentences=True)

    def xml_to_text(self, xml_string):
        root = ET.fromstring(xml_string)
        text = ""
        for elem in root.iter():
            if elem.text:
                text += elem.text + "\n"
        return text.strip()

    def process_xml_files(self):
        if not os.path.exists(self.output_directory):
            os.makedirs(self.output_directory)

        self.download()
        self.process_pdfs()

        namespace = {'tei': 'http://www.tei-c.org/ns/1.0'}

        for filename in os.listdir(self.xml_input_directory):
            if filename.endswith('.xml'):
                tree = ET.parse(os.path.join(self.xml_input_directory, filename))
                root = tree.getroot()
                year = filename.split('-')[0]
                level = filename.split('-')[1][1]

                Topic = ""
                Article_Name = ""
                Summary = ""

                title_element = root.find('.//tei:titleStmt/tei:title', {'tei': 'http://www.tei-c.org/ns/1.0'})

                for div in root.findall('.//tei:div', namespace):
                    head_element = div.find('.//tei:head', namespace)

                    if head_element is not None and "LEARNING OUTCOMES" in head_element.text:
                        Topic = prev_head_text if prev_head_text else head_element.text
                        try:
                            self.csv_data.pop()
                        except:
                            continue

                    Article_Name = head_element.text if head_element is not None else ""

                    Summary = ' '.join([p.text.strip() for p in div.findall('.//tei:p', namespace) if p.text is not None])
                    try:
                        if Topic == '' and "LEARNING OUTCOMES" in title_element.text:
                            head = title_element.text.replace(' LEARNING OUTCOMES', '')
                            Topic = head
                    except:
                        Topic = "N/A"
                    if Article_Name != "LEARNING OUTCOMES":
                        self.csv_data.append([level, Topic, year, Article_Name, Summary])

                    prev_head_text = head_element.text if head_element is not None else ""

    def generate_csv(self):
        file_name_ = os.path.basename(self.s3_url)
        csv_file_path = os.path.join(self.output_directory, f"{file_name_}.csv")

        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(['Level', 'Topic', 'Year', 'Article_Name', 'Summary'])
            csv_writer.writerows(self.csv_data)

        print("CSV file generated successfully.")

    def generate_metadata_csv(self):
        file_name_ = os.path.basename(self.s3_url)
        csv_file_path = os.path.join(self.output_directory, f"{file_name_}_metadata.csv")

        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)

        csv_file = open(csv_file_path, 'w', newline='', encoding='utf-8')
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Filename', 'Time', 'MD5 Identifier', 'Encoding Version', 'Lang', 'Application Identifier',
                             'Application Description', 'Application Version', 'Application Reference URL'])

        for filename in os.listdir(self.xml_input_directory):
            if filename.endswith('.xml'):
                tree = ET.parse(os.path.join(self.xml_input_directory, filename))
                root = tree.getroot()

                when_attribute = root.find('.//{http://www.tei-c.org/ns/1.0}appInfo/{http://www.tei-c.org/ns/1.0}application[@ident="GROBID"]')
                when_attribute = when_attribute.get('when') if when_attribute is not None else ""

                md5_identifier = root.find('.//{http://www.tei-c.org/ns/1.0}sourceDesc/{http://www.tei-c.org/ns/1.0}biblStruct/{http://www.tei-c.org/ns/1.0}idno[@type="MD5"]')
                md5_identifier = md5_identifier.text if md5_identifier is not None else ""

                version_attribute = root.get('encoding') if 'encoding' in root.attrib else "UTF-8"

                lang_attribute = root.get('lang') if 'lang' in root.attrib else "en"

                application_identifier = root.find('.//{http://www.tei-c.org/ns/1.0}appInfo/{http://www.tei-c.org/ns/1.0}application[@ident="GROBID"]')
                application_identifier = application_identifier.get('ident') if application_identifier is not None else ""

                application_description = root.find('.//{http://www.tei-c.org/ns/1.0}appInfo/{http://www.tei-c.org/ns/1.0}application[@ident="GROBID"]/{http://www.tei-c.org/ns/1.0}desc')
                application_description = application_description.text if application_description is not None else ""

                application_version = root.find('.//{http://www.tei-c.org/ns/1.0}appInfo/{http://www.tei-c.org/ns/1.0}application[@ident="GROBID"]')
                application_version = application_version.get('version') if application_version is not None else ""

                application_reference_url = root.find('.//{http://www.tei-c.org/ns/1.0}appInfo/{http://www.tei-c.org/ns/1.0}application[@ident="GROBID"]/{http://www.tei-c.org/ns/1.0}ref')
                application_reference_url = application_reference_url.get('target') if application_reference_url is not None else ""

                csv_writer.writerow([filename, when_attribute, md5_identifier, version_attribute, lang_attribute, application_identifier,
                                     application_description, application_version, application_reference_url])

        csv_file.close()

    def clear_directory(self, directory):
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Failed to delete {file_path}: {e}")

    def process_and_generate(self):
        self.process_xml_files()
        self.generate_csv()
        self.generate_metadata_csv()

        csv_contents = open(os.path.join(self.output_directory, f"{os.path.basename(self.s3_url)}.csv"), 'rb').read()
        metadata_csv_contents = open(os.path.join(self.output_directory, f"{os.path.basename(self.s3_url)}_metadata.csv"), 'rb').read()

        self.s3_upload(csv_contents, f"{os.path.basename(self.s3_url)}.csv", "raw_csv_grobid_output")
        self.s3_upload(metadata_csv_contents, f"{os.path.basename(self.s3_url)}_metadata.csv", "raw_csv_grobid_metadata")

        self.clear_directory(self.output_directory)
        self.clear_directory(self.input_directory)
