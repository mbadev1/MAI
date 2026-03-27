import requests
import json
import copy
import re
import datetime
import pandas as pd
from source.utils import has_explicit_repetitions

"""
Functions to interact with Clair API
"""

def activate_configuration(mode: str, language: str, keywords: list, host: str, token: str):
    # Define configuration
    agent_configuration = {
        "learning_space": "dev/clair-f2f",
        "is_active": True,
        "mode": mode,
        "language": language,
        "keywords": keywords
    }
    print(agent_configuration, flush=True)
    # Activate agent under this configuration
    req = requests.post(f"{host}/configuration", 
                       data=json.dumps(agent_configuration), 
                       headers={'Content-Type': 'application/json', 'access_token': token},
                       timeout=10)
    print(req, req.text, req.status_code, req.reason, flush=True)
    return req


def parse_turn(transcription: str, 
               dialogue: list, 
               turn_threshold: int = 5, 
               silence_threshold: int = 3):
    """
    Updates the dialogue based on the incoming transcription and handles the end of turns based on silence duration.

    Args:
    - transcription (str): The current chunk of transcribed text.
    - dialogue (list): Accumulated dialogue turns.
    - turn_threshold (int): Time in seconds to wait before considering a new turn based on the timestamp.
    - silence_threshold (int): Time in seconds to consider silence as the end of a turn.

    Returns:
    - A boolean indicating if a new turn was detected.
    - The updated dialogue list.
    """
    # One transcription can contain multiple parts splitted by "\n"
    # We need to check how many turns are in the transcription
    new_turn_info = [] # Store whether the multiple parts of the transcription are new turns
    
    # Initialize with default values if dialogue is empty
    if dialogue:
        last_timestamp = dialogue[-1]["timestamp"]
        last_user = dialogue[-1]["username"]
    else:
        last_timestamp = pd.Timestamp.now() - pd.Timedelta(seconds=turn_threshold + 1)  # Ensuring a new turn is detected for the first entry
        last_user = None

    # If the transcription is not empty
    if transcription.strip() and transcription != "Listening...":
        # Transcription may contain input from more than one person, this comes in a new line
        transcripts = transcription.split("\n")
        for transcript in transcripts:
            timestamp = pd.Timestamp.now()
            timedelta = (timestamp - last_timestamp).total_seconds()

            is_new_turn = True
            # Check if this transcript part is not a new turn
            items = transcript.split(": ")
            # if comes from the same user and within timedelta, its not a new turn
            if len(items) == 2:
                username, text = items
                if items[0] == last_user and timedelta < turn_threshold:
                    is_new_turn = False
            # if there is one part ("text") only, its not a new turn, its a continuation
            elif len(items) == 1:
                username = last_user
                text = transcript
                is_new_turn = False
            
            # if is a new turn, create a new one using the data
            if is_new_turn or len(dialogue)==0:
                dialogue.append({
                    "username": username,
                    "text": text.strip(),
                    "timestamp": timestamp
                })
            else: # if its not, append it to the previous item stored in dialogue
                dialogue[-1]["text"] += " " + text.strip()

            # Update variables for next iteration
            last_timestamp = timestamp
            last_user = username
            new_turn_info.append(is_new_turn)
    else:
        # Handling silence as a potential end of turn
        silence_duration = (pd.Timestamp.now() - last_timestamp).total_seconds()
        if silence_duration >= silence_threshold and dialogue:
            new_turn_info = []  # Indicate the potential end of a turn due to silence

    return new_turn_info, dialogue


def buffering_turn(transcription: str, 
                   dialogue: list, 
                   group: str, 
                   turn_threshold: int = 5, 
                   silence_threshold: int = 3,
                   last_processed_turn: dict = None,
                   verbose: bool = False):
    """
    Buffers transcription into turns, and returns turns ready for further processing.

    Args:
    - transcription (str): Incoming transcription text.
    - dialogue (list): Current state of the dialogue (list of turns).
    - group (str): Identifier for the group/conversation.
    - turn_threshold (int): Threshold in seconds for identifying new turns.
    - verbose (bool): If True, outputs additional logging information.

    Returns:
    - List of turns that are ready to be processed further.
    """
    if verbose:
        print("\n\t🎙️ Microphone input received 🎙️ ", flush=True)
        print('\n'.join([transcription[i:i+100] for i in range(0, len(transcription), 100)]), flush=True)

    new_turn_info, dialogue = parse_turn(transcription, dialogue, turn_threshold, silence_threshold)

    # Decide which turns are ready to be processed based on new_turn_info
    turns_to_process = []
    if new_turn_info:
        c = 1
        for t in new_turn_info:
            if t:
                if dialogue[-c]['username'] == 'Clair':
                    c += 1 # jump over Clair's turn
                turns_to_process.append(dialogue[-c])
                c += 1
    else: # silence detected
        c = 1
        if dialogue:
            if dialogue[-c]['username'] == 'Clair':
                c += 1
            if last_processed_turn and last_processed_turn != dialogue[-c]:
                last_processed_turn['last_turn'] = dialogue[-c]
                turns_to_process.append(last_processed_turn)
                
    if verbose and turns_to_process:
        for turn in turns_to_process:
            print_turn_info(turn, group)
        print_dialogue_info(dialogue)

    return turns_to_process

def print_turn_info(turn, group):
    """Prints detailed information about a single turn."""
    turn['group'] = group
    print("\n\t💬 A turn was completed 💬", flush=True)
    formatted_turn = copy.deepcopy(turn)
    if 'text' in formatted_turn  and len(formatted_turn['text']) > 50:
        formatted_turn['text'] = ' '.join([formatted_turn['text'][i:i+80] for i in range(0, len(formatted_turn['text']), 50)])
    json_str = json.dumps(formatted_turn, default=lambda obj: obj.strftime("%Y-%m-%d %H:%M:%S") if isinstance(obj, datetime.datetime) else type(obj).__name__, indent=2)
    print(json_str, flush=True)

def print_dialogue_info(dialogue):
    """Prints summary information about the current state of the dialogue."""
    print("\n\t🔄 Dialogue 🔄", flush=True)
    print("\tNumber of turns:", len(dialogue), flush=True)
    speakers = set([item['username'] for item in dialogue if 'username' in item])
    print("\tNumber of speakers:", len(speakers), flush=True)
    if dialogue:
        duration = dialogue[-1]['timestamp'] - dialogue[0]['timestamp']
        print("\tTime duration:", duration, flush=True)
    print("\t====================================\n", flush=True)

class RepetitionDetectedError(Exception):
    pass

def send_to_api_and_get_response(group: str = None, 
                                 username: str = None, 
                                 text: str = None, 
                                 timestamp: int = None, 
                                 dialogue: list = [], 
                                 host: str = None, 
                                 token: str = None,
                                 verbose: bool = True,
                                 **kwargs):
    
    # Skip sending to API if any of the required fields are missing
    if not group or not username or not text or not timestamp:
        return None
    else:
        print(f"\n\n>>>>Sending to API: {group}, {username}, {text}, {timestamp}\n\n", flush=True)

    # Look for repeated sequences (BUG in Whisper)
    if has_explicit_repetitions(text):
        raise RepetitionDetectedError("ERROR: The transcription contained repetitive sentences.\n\n")
    
    # Prepare the message to be sent to the API
    message = {
        "learning_space": "dev/clair-f2f",
        "group": group,
        "username": username,
        "text": text,
        "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S")
    }
    try:
        # Send the data to API with a POST request
        req = requests.post(f"{host}/message?retrieve_details=true&save=false",
                            data=json.dumps(message),
                            headers={'Content-Type': 'application/json', 'access_token': token},
                            timeout=20)
        output = req.json()
        # Combine the original transcription with the API response
        combined_output = {
            "transcription": f"{username} ({str(timestamp)[:19]}): {text}",
            "response": output['agent_intervention'],
            'selected_move': output['selected_move']
        }
    except:
        # If the request fails, raise information about the output of the api
        raise ValueError(f"ERROR: The API request failed. Output: {output}")

    if output['selected_move']:
        # Add the response to the dialogue
        dialogue.append({
            "group": group,
            "timestamp": timestamp,
            "username": "Clair",
            "text": output['selected_move'],
        })

    # Play audio file with the response
    if verbose and output['agent_intervention']:
        print("\n\t⭐ Clair's response ⭐", flush=True)
        print(json.dumps(combined_output, default=lambda obj: obj.strftime("%Y-%m-%d %H:%M:%S") if isinstance(obj, pd.Timestamp) else type(obj).__name__, indent=2), flush=True)
        print("\t====================================\n", flush=True)
    
    return json.dumps(combined_output, indent=2)
