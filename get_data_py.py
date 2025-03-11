import os
import textwrap
import json
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv
from extraction_schema import ExtractionSchema  # Import the Pydantic model

class HTMLExtractor:
    def __init__(self, html_dir=None, chunk_size=4000, output_file="output.json"):
        """Initialize the extractor with directory, chunk size, and output file using a Pydantic model for the schema."""
        load_dotenv()  # Load environment variables
        self.html_dir = os.getenv("DUMP_DIR") if html_dir is None else html_dir
        self.chunk_size = chunk_size
        
        # Use the Pydantic model to define the schema.
        self.schema_model = ExtractionSchema
        self.schema = list(self.schema_model.model_fields.keys())
        
        self.output_file = output_file
        self.extracted_data = self.load_existing_data()
        self.llm = ChatOpenAI(
            model_name="gpt-4o-mini",
            temperature=0,
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )

    def load_existing_data(self):
        """Load existing data from JSON to prevent overwriting previously extracted fields."""
        if os.path.exists(self.output_file):
            with open(self.output_file, "r", encoding="utf-8") as file:
                try:
                    return json.load(file)
                except json.JSONDecodeError:
                    return []
        return []

    def extract_text_from_html(self, file_path):
        """Extract meaningful text from an HTML file."""
        with open(file_path, "r", encoding="utf-8") as file:
            soup = BeautifulSoup(file, "html.parser")
            text = soup.get_text(separator="\n", strip=True)  # Extract readable text
        return text

    def split_into_chunks(self, text):
        """Split text into chunks while maintaining context."""
        return textwrap.wrap(text, self.chunk_size)

    def query_llm(self, prompt):
        """Send extracted text to an LLM and retrieve structured data in JSON format."""
        response = self.llm.invoke(prompt)
        try:
            content = response.content.strip()
            if content.startswith("```json"):
                content = content[7:-3].strip()
            return json.loads(content)
        except json.JSONDecodeError:
            print("⚠️ JSON parsing error. Returning empty dictionary.")
            return {}

    def process_html_file(self, file_path):
        """Process a single HTML file in chunks and update extracted data without overwriting existing fields."""
        print(f"📂 Processing: {file_path}")
        
        page_text = self.extract_text_from_html(file_path)
        chunks = self.split_into_chunks(page_text)
        
        extracted_fields = set()
        existing_entry = next((entry for entry in self.extracted_data if entry.get("file") == file_path), None)
        
        if existing_entry:
            extracted_data = existing_entry
            extracted_fields.update({key for key, value in extracted_data.items() if value})
        else:
            extracted_data = {"file": file_path}

        for i, chunk in enumerate(chunks):
            print(f"Sending Chunk {i+1}/{len(chunks)}...")

            remaining_fields = [field for field in self.schema if field not in extracted_fields]
            if not remaining_fields:
                break

            prompt = f"""
            Extract the following details from this web page in JSON format:
            {json.dumps(remaining_fields)}

            Requirements:
            - Return only a valid JSON object without any additional text or markdown formatting.
            - Ensure all fields are included. If a field is missing, return it with an empty string.
            - If multiple values are found for any field, return them as a list.
            
            HTML Content (Chunk {i+1}/{len(chunks)}):
            {chunk}
            """
            
            extracted_response = self.query_llm(prompt)
            
            for field in self.schema:
                if field in extracted_response and extracted_response[field]:
                    extracted_fields.add(field)
                    extracted_data[field] = extracted_response[field]
                    self.save_to_json()  # Save immediately after extracting a field
        for field in self.schema:
            if field not in extracted_data:
                extracted_data[field] = ""  # Ensure all fields are present
            elif isinstance(extracted_data[field], list) and not extracted_data[field]:
                extracted_data[field] = ""  # Convert empty lists to empty strings
        
        if existing_entry is None:
            self.extracted_data.append(extracted_data)
            self.save_to_json()

    def save_to_json(self):
        """Save extracted data to a JSON file, ensuring no overwrites of existing fields."""
        with open(self.output_file, "w", encoding="utf-8") as file:
            json.dump(self.extracted_data, file, indent=4)

    def process_all_html(self):
        """Process all HTML files and extract data in chunks, saving to JSON progressively."""
        for filename in os.listdir(self.html_dir):
            file_path = os.path.join(self.html_dir, filename)
            self.process_html_file(file_path)
            print(f"Extracted Data from {filename} saved to {self.output_file}")
            print("\n" + "="*80 + "\n")

# Run the extraction
if __name__ == "__main__":
    extractor = HTMLExtractor()
    extractor.process_all_html()
