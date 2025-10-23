from flask import Flask, request, jsonify
import hashlib
import sqlite3
from datetime import datetime
import os
import re

app = Flask(__name__)

# Database setup
DATABASE = 'strings.db'

def get_db():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS strings (
                id TEXT PRIMARY KEY,
                value TEXT UNIQUE NOT NULL,
                length INTEGER,
                is_palindrome INTEGER,
                unique_characters INTEGER,
                word_count INTEGER,
                sha256_hash TEXT,
                character_frequency_map TEXT,
                created_at TEXT
            )
        """)
        conn.commit()

# Initialize database on startup
init_db()

def analyze_string(value):
    """Analyze string and compute properties"""
    # Length
    length = len(value)

    # Is palindrome (case-insensitive)
    is_palindrome = value.lower() == value.lower()[::-1]

    # Unique characters
    unique_characters = len(set(value))

    # Word count
    word_count = len(value.split())

    # SHA-256 hash
    sha256_hash = hashlib.sha256(value.encode()).hexdigest()

    # Character frequency map
    char_freq = {}
    for char in value:
        char_freq[char] = char_freq.get(char, 0) + 1

    return {
        "length": length,
        "is_palindrome": is_palindrome,
        "unique_characters": unique_characters,
        "word_count": word_count,
        "sha256_hash": sha256_hash,
        "character_frequency_map": char_freq
    }

def parse_natural_language(query):
    """Parse natural language query into filters"""
    query_lower = query.lower()
    filters = {}

    # Check for palindrome
    if "palindrom" in query_lower:
        filters["is_palindrome"] = True

    # Check for word count
    if "single word" in query_lower:
        filters["word_count"] = 1
    elif "two word" in query_lower or "2 word" in query_lower:
        filters["word_count"] = 2

    # Check for length constraints
    length_pattern = r"longer than (\d+)"
    match = re.search(length_pattern, query_lower)
    if match:
        filters["min_length"] = int(match.group(1)) + 1

    length_pattern2 = r"shorter than (\d+)"
    match = re.search(length_pattern2, query_lower)
    if match:
        filters["max_length"] = int(match.group(1)) - 1

    # Check for character containment
    char_pattern = r"contain(?:s|ing)? (?:the )?(?:letter |character )?([a-z])"
    match = re.search(char_pattern, query_lower)
    if match:
        filters["contains_character"] = match.group(1)

    # Check for first vowel
    if "first vowel" in query_lower:
        filters["contains_character"] = "a"

    return filters

@app.route('/strings', methods=['POST'])
def create_string():
    """Create/Analyze String"""
    data = request.get_json()

    if not data:
        return jsonify({"error": "Invalid request body"}), 400

    if "value" not in data:
        return jsonify({"error": "Missing 'value' field"}), 400

    value = data["value"]

    if not isinstance(value, str):
        return jsonify({"error": "Invalid data type for 'value' (must be string)"}), 422

    # Analyze string
    properties = analyze_string(value)
    sha256_hash = properties["sha256_hash"]
    created_at = datetime.utcnow().isoformat() + "Z"

    # Store in database
    try:
        with get_db() as conn:
            conn.execute("""
                INSERT INTO strings (id, value, length, is_palindrome, unique_characters, 
                                   word_count, sha256_hash, character_frequency_map, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sha256_hash, value, properties["length"],
                int(properties["is_palindrome"]), properties["unique_characters"],
                properties["word_count"], sha256_hash,
                str(properties["character_frequency_map"]), created_at
            ))
            conn.commit()
    except sqlite3.IntegrityError:
        return jsonify({"error": "String already exists in the system"}), 409

    return jsonify({
        "id": sha256_hash,
        "value": value,
        "properties": properties,
        "created_at": created_at
    }), 201

@app.route('/strings/<path:string_value>', methods=['GET'])
def get_string(string_value):
    """Get Specific String"""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM strings WHERE value = ?", (string_value,)).fetchone()

    if not row:
        return jsonify({"error": "String does not exist in the system"}), 404

    return jsonify({
        "id": row["id"],
        "value": row["value"],
        "properties": {
            "length": row["length"],
            "is_palindrome": bool(row["is_palindrome"]),
            "unique_characters": row["unique_characters"],
            "word_count": row["word_count"],
            "sha256_hash": row["sha256_hash"],
            "character_frequency_map": eval(row["character_frequency_map"])
        },
        "created_at": row["created_at"]
    }), 200

@app.route('/strings', methods=['GET'])
def get_all_strings():
    """Get All Strings with Filtering"""
    # Get query parameters
    is_palindrome = request.args.get('is_palindrome')
    min_length = request.args.get('min_length')
    max_length = request.args.get('max_length')
    word_count = request.args.get('word_count')
    contains_character = request.args.get('contains_character')

    query = "SELECT * FROM strings WHERE 1=1"
    params = []
    filters_applied = {}

    try:
        if is_palindrome is not None:
            is_pal = is_palindrome.lower() == 'true'
            query += " AND is_palindrome = ?"
            params.append(int(is_pal))
            filters_applied["is_palindrome"] = is_pal

        if min_length is not None:
            query += " AND length >= ?"
            params.append(int(min_length))
            filters_applied["min_length"] = int(min_length)

        if max_length is not None:
            query += " AND length <= ?"
            params.append(int(max_length))
            filters_applied["max_length"] = int(max_length)

        if word_count is not None:
            query += " AND word_count = ?"
            params.append(int(word_count))
            filters_applied["word_count"] = int(word_count)

        if contains_character is not None:
            query += " AND value LIKE ?"
            params.append(f"%{contains_character}%")
            filters_applied["contains_character"] = contains_character
    except ValueError:
        return jsonify({"error": "Invalid query parameter values or types"}), 400

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()

    data = []
    for row in rows:
        data.append({
            "id": row["id"],
            "value": row["value"],
            "properties": {
                "length": row["length"],
                "is_palindrome": bool(row["is_palindrome"]),
                "unique_characters": row["unique_characters"],
                "word_count": row["word_count"],
                "sha256_hash": row["sha256_hash"],
                "character_frequency_map": eval(row["character_frequency_map"])
            },
            "created_at": row["created_at"]
        })

    return jsonify({
        "data": data,
        "count": len(data),
        "filters_applied": filters_applied
    }), 200

@app.route('/strings/filter-by-natural-language', methods=['GET'])
def filter_by_natural_language():
    """Natural Language Filtering"""
    query_param = request.args.get('query')

    if not query_param:
        return jsonify({"error": "Missing 'query' parameter"}), 400

    # Parse natural language
    filters = parse_natural_language(query_param)

    if not filters:
        return jsonify({"error": "Unable to parse natural language query"}), 400

    # Build SQL query
    sql_query = "SELECT * FROM strings WHERE 1=1"
    params = []

    if "is_palindrome" in filters:
        sql_query += " AND is_palindrome = ?"
        params.append(int(filters["is_palindrome"]))

    if "min_length" in filters:
        sql_query += " AND length >= ?"
        params.append(filters["min_length"])

    if "max_length" in filters:
        sql_query += " AND length <= ?"
        params.append(filters["max_length"])

    if "word_count" in filters:
        sql_query += " AND word_count = ?"
        params.append(filters["word_count"])

    if "contains_character" in filters:
        sql_query += " AND value LIKE ?"
        params.append(f"%{filters['contains_character']}%")

    with get_db() as conn:
        rows = conn.execute(sql_query, params).fetchall()

    data = []
    for row in rows:
        data.append({
            "id": row["id"],
            "value": row["value"],
            "properties": {
                "length": row["length"],
                "is_palindrome": bool(row["is_palindrome"]),
                "unique_characters": row["unique_characters"],
                "word_count": row["word_count"],
                "sha256_hash": row["sha256_hash"],
                "character_frequency_map": eval(row["character_frequency_map"])
            },
            "created_at": row["created_at"]
        })

    return jsonify({
        "data": data,
        "count": len(data),
        "interpreted_query": {
            "original": query_param,
            "parsed_filters": filters
        }
    }), 200

@app.route('/strings/<path:string_value>', methods=['DELETE'])
def delete_string(string_value):
    """Delete String"""
    with get_db() as conn:
        cursor = conn.execute("DELETE FROM strings WHERE value = ?", (string_value,))
        conn.commit()

        if cursor.rowcount == 0:
            return jsonify({"error": "String does not exist in the system"}), 404

    return '', 204

if __name__ == '__main__':
    app.run(debug=True)
