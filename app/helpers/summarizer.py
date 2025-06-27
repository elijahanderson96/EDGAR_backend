import requests  # Still used by openai library, or can be removed if openai handles all http internally
import json
from bs4 import BeautifulSoup
import openai  # Added for OpenAI API interaction

# --- Configuration ---
# TODO: Update this URL to your llama.cpp OpenAI-compatible API base URL
OPENAI_API_BASE_URL = "http://localhost:8080/v1"
# TODO: Update this if your server requires a specific API key
OPENAI_API_KEY = "sk-no-key-required"
# TODO: Update this to the model name/alias your llama.cpp server uses for the desired GGUF file
MODEL_NAME = "unsloth_Qwen3-30B-A3B-GGUF_Qwen3-30B-A3B-UD-Q6_K_XL.gguf"

# TODO: Update this path to your quarterly report HTML file
REPORT_FILE_PATH = "csiq_report.htm"  # User updated this path

# Parameters for the OpenAI Chat Completions API
LLM_PARAMETERS = {
    "max_tokens": 1024, # Max tokens is often not needed or handled differently with streaming
    # The model will stop on its own, or you can use 'stop' sequences.
    "temperature": 0.7,
    "stream": True,  # <<<< EDITED: Changed to True for streaming
    # "top_p": 1.0,
    # "stop": None,
}

# Optional: Define a system prompt
SYSTEM_PROMPT = ("You are a helpful AI assistant. Your task is to analyze the provided text from a quarterly report.")


def read_and_parse_html_report(file_path: str) -> str | None:
    """
    Reads an HTML file, parses it, removes table content, and extracts all other text.

    Args:
        file_path (str): The path to the HTML file.

    Returns:
        str | None: The extracted text content (excluding tables), or None if an error occurs.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()

        soup = BeautifulSoup(html_content, 'html.parser')

        for table in soup.find_all('table'):
            table.decompose()

        report_text = soup.get_text(separator=' ', strip=True)

        if not report_text.strip():
            print(f"Warning: No text content found in '{file_path}' after removing tables.")

        print(f"Successfully read and parsed HTML from: {file_path}")
        return report_text

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
    except ImportError:
        print("Error: BeautifulSoup4 library is not installed. Please install it with 'pip install beautifulsoup4'")
        return None
    except Exception as e:
        print(f"Error reading or parsing HTML file '{file_path}': {e}")
        return None


def send_text_to_openai_api(text_content: str, model_name: str, api_base_url: str, api_key: str, system_prompt: str,
                            params: dict) -> str | None:
    """
    Sends the given text content as a user message to the OpenAI-compatible API.
    If params["stream"] is True, it prints the response chunks as they arrive and returns the full concatenated response.
    Otherwise, it returns the complete response content directly.

    Args:
        text_content (str): The text content forming the user's message.
        model_name (str): The name/alias of the model to use.
        api_base_url (str): The base URL of the OpenAI-compatible API.
        api_key (str): The API key.
        system_prompt (str): The system message to guide the assistant.
        params (dict): A dictionary of parameters for the Chat Completions API.

    Returns:
        str | None: The content of the assistant's response, or None if an error occurs.
    """
    if not text_content or not text_content.strip():
        print("Error: No text content to send (it might be empty after HTML parsing).")
        return None

    try:
        client = openai.OpenAI(
            base_url=api_base_url,
            api_key=api_key,
        )

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": text_content})

        print(f"Sending text to LLM (model: {model_name}) via OpenAI API at {api_base_url}...")

        stream_response = params.get("stream", False)  # Check if streaming is requested

        completion_stream = client.chat.completions.create(
            model=model_name,
            messages=messages,
            **params
        )

        if stream_response:
            print("\n--- LLM Streamed Response ---")
            full_response_content = []
            for chunk in completion_stream:
                if chunk.choices and chunk.choices[0].delta and chunk.choices[0].delta.content:
                    content_piece = chunk.choices[0].delta.content
                    print(content_piece, end="", flush=True)  # Print piece immediately
                    full_response_content.append(content_piece)
            print()  # Add a newline after the stream finishes
            print("Successfully received and streamed response from LLM.")
            return "".join(full_response_content) if full_response_content else None
        else:  # Non-streaming case (though params["stream"] is now True by default)
            if completion_stream.choices and completion_stream.choices[0].message:
                assistant_response = completion_stream.choices[0].message.content
                print("Successfully received response from LLM.")
                return assistant_response
            else:
                print("Error: No response content found in completion object.")
                print(f"Full API response: {completion_stream.model_dump_json(indent=2)}")
                return None

    except openai.APIConnectionError as e:
        print(f"Error connecting to OpenAI API at '{api_base_url}': {e}")
        return None
    except openai.APIStatusError as e:
        print(f"OpenAI API returned an API Error: Status {e.status_code}, Response: {e.response}")
        return None
    except openai.RateLimitError as e:
        print(f"OpenAI API request exceeded rate limit: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while sending text to LLM via OpenAI API: {e}")
        return None


def main():
    """
    Main function to read the HTML report, extract text, and send it to the LLM
    using an OpenAI-compatible API, with support for streaming.
    """
    print("--- Starting Quarterly Report (HTML) Upload to Local LLM (OpenAI API) ---")

    # 1. Read and parse the HTML report text, excluding tables
    report_text_from_html = read_and_parse_html_report(REPORT_FILE_PATH)

    if report_text_from_html is None:
        print("Halting script due to error in reading or parsing the report file.")
        return

    if not report_text_from_html.strip():
        print("No text content was extracted from the HTML (excluding tables). Halting script.")
        return

    # 2. Prepare the instructional prompt (this will be the user message)
    instructional_prompt_for_llm = (
        f"Please analyze the following quarterly report content and provide a summary "
        f"of its key financial highlights and challenges:\n\n{report_text_from_html}"
    )

    # 3. Send the extracted text to the LLM via the OpenAI-compatible API
    # The send_text_to_openai_api function will print the stream directly.
    # It will also return the full concatenated content.
    llm_full_response = send_text_to_openai_api(
        text_content=instructional_prompt_for_llm,
        model_name=MODEL_NAME,
        api_base_url=OPENAI_API_BASE_URL,
        api_key=OPENAI_API_KEY,
        system_prompt=SYSTEM_PROMPT,
        params=LLM_PARAMETERS
    )

    if llm_full_response:
        # Optional: You can do something with the llm_full_response here if needed,
        # for example, save it to a file, though it has already been printed to the console.
        # print("\n--- Full Concatenated LLM Response (for reference) ---")
        # print(llm_full_response)
        pass  # Already printed by the streaming function
    else:
        # This part will be reached if there was an error or no content in the stream
        print("Failed to get a complete response from the LLM or stream was empty.")

    print("\n--- Script Finished ---")


if __name__ == "__main__":
    # Before running:
    # 1. Ensure your llama.cpp server is running with OpenAI API compatibility enabled
    #    and accessible at OPENAI_API_BASE_URL. It must support streaming.
    # 2. Make sure REPORT_FILE_PATH points to your actual report HTML file.
    # 3. Install the required Python libraries:
    #    pip install openai beautifulsoup4 requests
    main()
