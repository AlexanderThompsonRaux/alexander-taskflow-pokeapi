# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from typing import List, Optional
import uvicorn

# --- Constants ---
DB_NAME = "pokemon_assessment.db"


# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    connection = None
    try:
        # --- Implement Here ---
        connection = sqlite3.connect(DB_NAME)
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return connection


# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
      Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---

        # Imports are placed here because of "code inside blocks" instruction.
        # I am using the API provided to create a source of truth for spelling corrections.
        import requests
        import re 

        # Define the base URL for the external Pokémon API.
        POKEAPI_BASE_URL = "https://pokeapi.co/api/v2/"

        
        # Essential for data integrity; SQLite enforces FK constraints.
        cursor.execute("PRAGMA foreign_keys = ON;")


        # --- Helper Functions (Usually would be global but Defined here because of "code inside blocks" instruction) ---

        # Helper Function: _fetch_all_pokeapi_resources
        # Fetches all resource names (pokemon, types, abilities) from the PokeAPI.
        # Create a canonical list of valid names for validation and correction.
        def _fetch_all_pokeapi_resources(endpoint_name: str) -> List[str]:
            url = f"{POKEAPI_BASE_URL}{endpoint_name}/?limit=10000" 
            names = set()
            while url:
                try:
                    response = requests.get(url, timeout=10)
                    response.raise_for_status()
                    data = response.json()
                    for item in data['results']:
                        names.add(item['name']) # Add name to the set
                    url = data['next']
                except requests.exceptions.RequestException as e:
                    print(f"  Warning: Error fetching from PokeAPI endpoint '{endpoint_name}': {e}. Cleaning will proceed without external data for this category.")
                    return []
            return sorted(list(names)) # Convert set to list and sort


        # Helper Function: to_title_case
        # Standardizes casing to Title Case, specifically handling hyphens by converting them to spaces.
        def to_title_case(text: str) -> str:
            if not text:
                return text
            return ' '.join(word.capitalize() for word in text.replace('-', ' ').split(' '))


        # Helper Function: normalize_string
        # Prepares a string for comparison by removing all non-alphanumeric characters and converting to lowercase.
        def normalize_string(s: str) -> str:
            if not s:
                return ""
            return re.sub(r'[^a-zA-Z0-9]', '', s).lower()

        # Helper Function: levenshtein_distance
        # Calculates the Levenshtein distance between two strings.
        #  - Levenshtein: Gives a number of single-character edits (insertions, deletions, substitutions)
        #                 required to change one word into the other.
        def levenshtein_distance(s1: str, s2: str) -> int:
            if len(s1) < len(s2):
                return levenshtein_distance(s2, s1)
            
            if len(s2) == 0:
                return len(s1)
            
            previous_row = range(len(s2) + 1)
            for i, c1 in enumerate(s1):
                current_row = [i + 1]
                for j, c2 in enumerate(s2):
                    insertions = previous_row[j + 1] + 1 
                    deletions = current_row[j] + 1
                    substitutions = previous_row[j] + (c1 != c2)

                    current_row.append(min(insertions, deletions, substitutions))
                previous_row = current_row
            
            return previous_row[-1]


        # Helper Function: nullify_fks_referencing_id
        def nullify_fks_referencing_id(deleted_id: int, referenced_table_name: str):
            for fk_table, fk_column, ref_table in fk_relationships:
                if ref_table == referenced_table_name:
                    try:
                        cursor.execute(f"""
                            UPDATE {fk_table}
                            SET {fk_column} = NULL
                            WHERE {fk_column} = ?;
                        """, (deleted_id,))
                        if cursor.rowcount > 0:
                            print(f"    - Nullified {cursor.rowcount} FKs in '{fk_table}.{fk_column}' referencing deleted ID {deleted_id} from '{referenced_table_name}'.")
                    except sqlite3.OperationalError as e:
                        if "no such column" in str(e) or "no such table" in str(e):
                            pass
                        else:
                            raise


        # --- Step 1: Fetch and Prepare Canonical Lists ---

        # Get Accurate data from PokeAPI and create Base lists.
        print("1. Fetching valid names from PokeAPI and static lists...")
        raw_valid_pokemon_names_api = _fetch_all_pokeapi_resources('pokemon')
        raw_valid_type_names_api = _fetch_all_pokeapi_resources('type')
        raw_valid_ability_names_api = _fetch_all_pokeapi_resources('ability')

        # Define a static list of known trainer names. Api does not provide these.
        raw_valid_trainer_names_list = ["Ash Ketchum", "Brock", "Gary Oak", "Professor Oak", "Misty"] # Add Any names as required

        # Create 'valid_map' dictionaries for each entity type.
        # These maps link a normalized (lowercase, alphanumeric-only) version of a name
        # to its canonical, Title Cased form. This is for fuzzy matching.
        valid_pokemon_map = {normalize_string(name): to_title_case(name) for name in raw_valid_pokemon_names_api}
        valid_type_map = {normalize_string(name): to_title_case(name) for name in raw_valid_type_names_api}
        valid_ability_map = {normalize_string(name): to_title_case(name) for name in raw_valid_ability_names_api}
        valid_trainer_map = {normalize_string(name): to_title_case(name) for name in raw_valid_trainer_names_list}

        # Create 'original_valid_set' sets for quick, exact validation of Title Cased names.
        # These sets contain the final version of the names.
        original_valid_pokemon_names_set = {to_title_case(name) for name in raw_valid_pokemon_names_api}
        original_valid_type_names_set = {to_title_case(name) for name in raw_valid_type_names_api}
        original_valid_ability_names_set = {to_title_case(name) for name in raw_valid_ability_names_api}
        original_valid_trainer_names_set = {to_title_case(name) for name in raw_valid_trainer_names_list}

        # Create metadata for each table, including their valid maps, sets, and a Levenshtein distance threshold.
        # NB - The threshold can be updated to cater for a wider variety of changes when using levenshtein_distance func.
        tables_meta = {
            'pokemon': {'valid_map': valid_pokemon_map, 'original_valid_set': original_valid_pokemon_names_set, 'threshold': 2},
            'types': {'valid_map': valid_type_map, 'original_valid_set': original_valid_type_names_set, 'threshold': 2},
            'abilities': {'valid_map': valid_ability_map, 'original_valid_set': original_valid_ability_names_set, 'threshold': 2},
            'trainers': {'valid_map': valid_trainer_map, 'original_valid_set': original_valid_trainer_names_set, 'threshold': 2}
        }

        # Define foreign key relationships in the database schema.
        # Format: (Table_With_FK, FK_Column_Name, Table_Referenced_By_FK)
        fk_relationships = [
            ('pokemon', 'type1_id', 'types'),
            ('pokemon', 'type2_id', 'types'),
            ('trainer_pokemon_abilities', 'pokemon_id', 'pokemon'),
            ('trainer_pokemon_abilities', 'trainer_id', 'trainers'),
            ('trainer_pokemon_abilities', 'ability_id', 'abilities'),
        ]


        # --- Step 2: Correct Misspellings and Standardize Casing ---

        # Iterate through each relevant table (pokemon, types, abilities, trainers) to:
        # - Identify and remove entries that are purely junk (e.g., empty strings, "---").
        # - Apply fuzzy matching to correct misspellings to their closest canonical name.
        # - Standardize the casing of all names to Title Case.
        print("2. Correcting misspellings (fuzzy matching) and standardizing casing...")

        junk_pattern_global = re.compile(r'^\W*$|^\s*$', re.UNICODE) 

        for table_name, meta in tables_meta.items():# Iterate through each table (pokemon, types, abilities, trainers)
            valid_map = meta['valid_map'] 
            threshold = meta['threshold']
            
            cursor.execute(f"SELECT id, name FROM {table_name}")
            rows_to_process = cursor.fetchall()
            
            updates = []
            deletions_from_step2 = []

            for row_id, current_name_db in rows_to_process:
                # Check if the current name is junk (empty, whitespace, non-alphanumeric).
                if not current_name_db or not str(current_name_db).strip() or junk_pattern_global.match(str(current_name_db)):
                    deletions_from_step2.append((row_id, current_name_db))
                    continue

                normalized_current_name_db = normalize_string(current_name_db)
                
                proposed_new_name = to_title_case(current_name_db) 
                min_distance = float('inf')
                
                confident_match_found = False

                # Priority 1: Check for an exact match.
                if normalized_current_name_db in valid_map:
                    proposed_new_name = valid_map[normalized_current_name_db]
                    confident_match_found = True
                else:
                    # Priority 2: If no exact match, perform fuzzy search using Levenshtein distance.
                    for valid_normalized_name, valid_title_case_name in valid_map.items():
                        dist = levenshtein_distance(normalized_current_name_db, valid_normalized_name)
                        
                        if dist < min_distance:
                            min_distance = dist
                            proposed_new_name = valid_title_case_name
                            confident_match_found = True
                        elif dist == min_distance and len(valid_normalized_name) < len(normalize_string(proposed_new_name)):
                             proposed_new_name = valid_title_case_name
                             confident_match_found = True
                    
                    if min_distance > threshold:
                        proposed_new_name = to_title_case(current_name_db) 
                        confident_match_found = False # No confident fuzzy match found

                
                if proposed_new_name != current_name_db:
                    updates.append((row_id, proposed_new_name, current_name_db))
            
            # Now, apply all collected deletions and updates for the current table.
            # Handle deletions of junk/empty entries first to prevent FK issues.
            for row_id_to_delete, original_name_for_print in deletions_from_step2:
                nullify_fks_referencing_id(row_id_to_delete, table_name) # Nullify FKs before deleting the primary key row
                cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (row_id_to_delete,))
                print(f"  - Removed junk/empty entry '{original_name_for_print}' (ID: {row_id_to_delete}) from '{table_name}'.")

            # Apply name correction/casing updates.
            for row_id, new_name, original_name_for_print in updates:
                current_db_name_after_updates = cursor.execute(f"SELECT name FROM {table_name} WHERE id = ?", (row_id,)).fetchone()
                if current_db_name_after_updates and current_db_name_after_updates[0] != new_name:
                     cursor.execute(f"UPDATE {table_name} SET name = ? WHERE id = ?", (new_name, row_id))
                     print(f"  - Corrected/Cased '{original_name_for_print}' to '{new_name}' in '{table_name}'.")


        # --- Step 3: Dynamic Removal of Redundant/Invalid Entries and Non-API/Static List Items ---

        # This catches any entries that were not correctable by fuzzy matching but are still invalid.
        print("3. Removing unvalidated entries not in canonical lists...")
        for table_name, meta in tables_meta.items():# Iterate through each table (pokemon, types, abilities, trainers)
            original_valid_set = meta['original_valid_set'] 
            
            cursor.execute(f"SELECT id, name FROM {table_name}")
            items_to_check = cursor.fetchall()
            
            ids_to_delete_step3 = []

            for item_id, item_name in items_to_check:
                # If an item's name is not found in our set of *original, Title Cased valid names*,
                # it means it's an unvalidated or truly invalid entry.
                if item_name not in original_valid_set:
                    ids_to_delete_step3.append((item_id, item_name))

            if ids_to_delete_step3:
                for item_id, name_to_report in ids_to_delete_step3:
                    nullify_fks_referencing_id(item_id, table_name) # Nullify FKs before deleting the primary key row
                    cursor.execute(f"DELETE FROM {table_name} WHERE id = ?", (item_id,))
                    print(f"  - Removed unvalidated entry '{name_to_report}' (ID: {item_id}) from '{table_name}'.")
                print(f"  - Executed deletion of {len(ids_to_delete_step3)} entries from '{table_name}'.")


        # --- Step 4: Remove Duplicate Entries and Remap Foreign Keys ---
        
        # Remap FKs to the canonical (lowest ID) record, then delete duplicates.
        print("4. Removing duplicate entries and remapping foreign keys...")

        for table_name in tables_meta.keys(): # Iterate through each table (pokemon, types, abilities, trainers)
            cursor.execute(f"""
                SELECT name, MIN(id) as canonical_id, GROUP_CONCAT(id) as duplicate_ids
                FROM {table_name}
                GROUP BY LOWER(name)
                HAVING COUNT(*) > 1;
            """)
            duplicates_info = cursor.fetchall()

            for original_name, canonical_id, duplicate_ids_str in duplicates_info:
                duplicate_ids_to_remove = [int(x) for x in duplicate_ids_str.split(',') if int(x) != canonical_id]
                
                if not duplicate_ids_to_remove:
                    continue

                print(f"  - Found duplicates for '{original_name}' in '{table_name}'. Canonical ID: {canonical_id}, Duplicates to remove: {duplicate_ids_to_remove}")

                for fk_table, fk_column, referenced_pk_table_name in fk_relationships:
                    if referenced_pk_table_name == table_name:
                        placeholders = ','.join('?' * len(duplicate_ids_to_remove))
                        try:
                            cursor.execute(f"""
                                UPDATE {fk_table}
                                SET {fk_column} = ?
                                WHERE {fk_column} IN ({placeholders});
                            """, (canonical_id, *duplicate_ids_to_remove))
                            if cursor.rowcount > 0:
                                print(f"    - Remapped {cursor.rowcount} FKs in '{fk_table}.{fk_column}' from {duplicate_ids_to_remove} to {canonical_id}.")
                        except sqlite3.OperationalError as e:
                            if "no such column" in str(e) or "no such table" in str(e):
                                pass
                            else:
                                raise
                        except Exception as e:
                            print(f"    - Error during FK remapping for '{fk_table}.{fk_column}': {e}")
                            raise

                # Delete the duplicate primary key rows.
                placeholders = ','.join('?' * len(duplicate_ids_to_remove))
                cursor.execute(f"""
                    DELETE FROM {table_name}
                    WHERE id IN ({placeholders});
                """, duplicate_ids_to_remove)
                print(f"  - Deleted {cursor.rowcount} duplicate entries from '{table_name}' for '{original_name}'.")


        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()  # Roll back changes on error

# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root response message
        Return a simple JSON response object that contains a `message` key with any corresponding value.
        """
        # --- Implement here ---
        return {"message": "Welcome to the Pokémon Trainer API! Database has been cleaned."}
        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokémon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---
        pokemon_names = [] 
        conn = None
        try:
            conn = connect_db()
            if conn is None:
                raise HTTPException(status_code=500, detail="Database connection failed.")
            
            conn.row_factory = sqlite3.Row 
            cursor = conn.cursor()
            
            # SQL query to select distinct Pokémon names linked to a specific ability.
            # LOWER() ensures a case-insensitive search.
            cursor.execute("""
                SELECT DISTINCT p.name
                FROM pokemon p
                JOIN trainer_pokemon_abilities tpa ON p.id = tpa.pokemon_id
                JOIN abilities a ON tpa.ability_id = a.id
                WHERE LOWER(a.name) = LOWER(?);
            """, (ability_name,))

            rows = cursor.fetchall()
            if rows:
                pokemon_names = [row['name'] for row in rows]
            
        except sqlite3.Error as e:
            
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        except Exception as e:
            
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
        finally:
            if conn:
                conn.close()
        
        return pokemon_names

        # --- NB: Future expansion - Enable Fuzzy search on API inputs for robustness (e.g., 'fira' -> 'Fire'). ---
        #
        # Strategy:
        # 1. `clean_database` returns `tables_meta`.
        # 2. Pass `tables_meta` to app.
        # 3. Helper `get_closest_canonical_name` uses `normalize_string`/`levenshtein_distance`(Moved to global helper function).
        # 4. Use canonical name in endpoint queries.
        #
        # Leverages cleaning phase data for better UX allowing users to search on misspelled names.


        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokémon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---
        pokemon_names = []
        conn = None
        try:
            conn = connect_db()
            if conn is None:
                raise HTTPException(status_code=500, detail="Could not connect to the database.")
            
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Query to find Pokémon names by type, considering both type1_id and type2_id
            cursor.execute("""
                SELECT DISTINCT p.name
                FROM pokemon p
                JOIN types t1 ON p.type1_id = t1.id
                LEFT JOIN types t2 ON p.type2_id = t2.id
                WHERE LOWER(t1.name) = LOWER(?) OR (t2.id IS NOT NULL AND LOWER(t2.name) = LOWER(?));
            """, (type_name, type_name))

            rows = cursor.fetchall()
            if rows:
                pokemon_names = [row['name'] for row in rows]
            
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
        finally:
            if conn:
                conn.close()
        
        return pokemon_names
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 6: Retrieve all trainer names who have a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist or has no trainer.
        """
        # --- Implement here ---
        trainer_names = []
        conn = None
        try:
            conn = connect_db()
            if conn is None:
                raise HTTPException(status_code=500, detail="Could not connect to the database.")
            
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Query to find trainer names who have a specific Pokémon
            cursor.execute("""
                SELECT DISTINCT t.name
                FROM trainers t
                JOIN trainer_pokemon_abilities tpa ON t.id = tpa.trainer_id
                JOIN pokemon p ON tpa.pokemon_id = p.id
                WHERE LOWER(p.name) = LOWER(?);
            """, (pokemon_name,))

            rows = cursor.fetchall()
            if rows:
                trainer_names = [row['name'] for row in rows]
            
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
        finally:
            if conn:
                conn.close()
        
        return trainer_names
        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all ability names of a specific Pokémon.
        Query the cleaned database. Handle cases where the Pokémon doesn't exist.
        """
        # --- Implement here ---
        ability_names = []
        conn = None
        try:
            conn = connect_db()
            if conn is None:
                raise HTTPException(status_code=500, detail="Could not connect to the database.")
            
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Query to find ability names for a specific Pokémon
            cursor.execute("""
                SELECT DISTINCT a.name
                FROM abilities a
                JOIN trainer_pokemon_abilities tpa ON a.id = tpa.ability_id
                JOIN pokemon p ON tpa.pokemon_id = p.id
                WHERE LOWER(p.name) = LOWER(?);
            """, (pokemon_name,))

            rows = cursor.fetchall()
            if rows:
                ability_names = [row['name'] for row in rows]
            
        except sqlite3.Error as e:
            raise HTTPException(status_code=500, detail=f"Database error: {e}")
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred: {e}")
        finally:
            if conn:
                conn.close()
        
        return ability_names
        # --- End Implementation ---

    # --- Implement Task 8 here ---
    @app.post("/pokemon/create/{pokemon_name}")
    async def create_pokemon_record(pokemon_name: str) -> dict: # async for I/O bound tasks like API calls
        """
        Task 8: Create a new Pokémon record in the trainer_pokemon_abilities table.
        Fetches Pokémon data from PokeAPI, creates/references foreign keys in local DB,
        and returns the new record's database ID(s) in trainer_pokemon_abilities.
        """
        
        # Duplicated instead of specified globally, to as to stay inside Implementation Blocks
        import requests 

        # Helper Function: to_title_case
        # Standardizes string casing to Title Case, specifically handling hyphens by converting them to spaces.
        # Duplicated here to as to stay inside Implementation Blocks
        def to_title_case(text: str) -> str:
            if not text:
                return text
            # Replace hyphens with spaces, then capitalize the first letter of each word.
            return ' '.join(word.capitalize() for word in text.replace('-', ' ').split(' '))
        

        conn = None
        tpa_ids = []
        try:
            conn = connect_db() # 1. Establish database connection
            if conn is None:
                raise HTTPException(status_code=500, detail="Could not connect to the database.")
            
            cursor = conn.cursor()
            cursor.execute("PRAGMA foreign_keys = ON;")

            # 2. Fetch Pokémon data from PokeAPI
            pokeapi_url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name.lower()}" # PokeAPI uses lowercase names
            
            try:
                response = requests.get(pokeapi_url, timeout=10)
                response.raise_for_status()
                poke_data = response.json()
            except requests.exceptions.RequestException as e:
                if response.status_code == 404:
                    raise HTTPException(status_code=404, detail=f"Pokémon '{pokemon_name}' not found on PokeAPI.")
                else:
                    raise HTTPException(status_code=500, detail=f"Error fetching data from PokeAPI: {e}")

            # Extract and standardize Pokémon data from PokeAPI response
            pokemon_name_api = to_title_case(poke_data['name'])
            types_api = [to_title_case(t['type']['name']) for t in poke_data['types']]
            abilities_api = [to_title_case(a['ability']['name']) for a in poke_data['abilities']]

            # 3. Get or Create Type IDs in our local database
            type1_id = None
            type2_id = None
            if types_api:
                # Process primary type (type1)
                cursor.execute("SELECT id FROM types WHERE LOWER(name) = LOWER(?)", (types_api[0],))
                type_row = cursor.fetchone()
                if type_row:
                    type1_id = type_row[0] # Use existing ID
                else:
                    cursor.execute("INSERT INTO types (name) VALUES (?)", (types_api[0],))
                    type1_id = cursor.lastrowid # Get ID of newly inserted type
                
                # Process secondary type (type2), if it exists
                if len(types_api) > 1:
                    cursor.execute("SELECT id FROM types WHERE LOWER(name) = LOWER(?)", (types_api[1],))
                    type_row = cursor.fetchone()
                    if type_row:
                        type2_id = type_row[0] # Use existing ID
                    else:
                        cursor.execute("INSERT INTO types (name) VALUES (?)", (types_api[1],))
                        type2_id = cursor.lastrowid # Get ID of newly inserted type

            # 4. Get or Create Ability IDs in our local database
            ability_ids_local = []
            for ability_name_api in abilities_api:
                cursor.execute("SELECT id FROM abilities WHERE LOWER(name) = LOWER(?)", (ability_name_api,))
                ability_row = cursor.fetchone()
                if ability_row:
                    ability_ids_local.append(ability_row[0]) # Use existing ID
                else:
                    cursor.execute("INSERT INTO abilities (name) VALUES (?)", (ability_name_api,))
                    ability_ids_local.append(cursor.lastrowid) # Get ID of newly inserted ability

            # 5. Get or Create Pokémon record in our local database
            pokemon_id = None
            cursor.execute("SELECT id FROM pokemon WHERE LOWER(name) = LOWER(?)", (pokemon_name_api,))
            pokemon_row = cursor.fetchone()
            if pokemon_row:
                pokemon_id = pokemon_row[0]
                # If Pokémon already exists, update its types in case they changed or were missing
                cursor.execute(
                    "UPDATE pokemon SET type1_id = ?, type2_id = ? WHERE id = ?",
                    (type1_id, type2_id, pokemon_id)
                )
            else:
                cursor.execute(
                    "INSERT INTO pokemon (name, type1_id, type2_id) VALUES (?, ?, ?)",
                    (pokemon_name_api, type1_id, type2_id)
                )
                pokemon_id = cursor.lastrowid

            if pokemon_id is None:
                raise HTTPException(status_code=500, detail="Failed to create or retrieve Pokémon record ID.")

            # 6. Randomly assign a trainer
            trainer_id = None
            cursor.execute("SELECT id FROM trainers ORDER BY RANDOM() LIMIT 1") # Select a random trainer ID
            trainer_row = cursor.fetchone()
            if trainer_row:
                trainer_id = trainer_row[0]
            else:
                print("No trainers found. Creating a default 'New Trainer'.")
                cursor.execute("INSERT INTO trainers (name) VALUES (?)", ("New Trainer",))
                trainer_id = cursor.lastrowid # Get ID of newly inserted trainer
            
            if trainer_id is None:
                raise HTTPException(status_code=500, detail="Failed to assign or create a trainer ID.")

            # 7. Insert into trainer_pokemon_abilities table for each ability
            for ab_id in ability_ids_local:
                cursor.execute(
                    "SELECT id FROM trainer_pokemon_abilities WHERE trainer_id = ? AND pokemon_id = ? AND ability_id = ?",
                    (trainer_id, pokemon_id, ab_id)
                )
                existing_tpa_record = cursor.fetchone()
                if not existing_tpa_record:
                    cursor.execute(
                        "INSERT INTO trainer_pokemon_abilities (trainer_id, pokemon_id, ability_id) VALUES (?, ?, ?)",
                        (trainer_id, pokemon_id, ab_id)
                    )
                    tpa_ids.append(cursor.lastrowid)
                else:
                    tpa_ids.append(existing_tpa_record[0])

            conn.commit()

            # 8. Return the ID(s) of the trainer_pokemon_abilities records
            return {"tpa_ids": tpa_ids}

        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            raise HTTPException(status_code=500, detail=f"Database error during record creation: {e}")
        except Exception as e:
            if conn:
                conn.rollback()
            raise HTTPException(status_code=500, detail=f"An unexpected error occurred during record creation: {e}")
        finally:
            if conn:
                conn.close()
        # --- End Implementation ---

    print("FastAPI app created successfully.")
    return app


# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)
        temp_conn.close()

    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
