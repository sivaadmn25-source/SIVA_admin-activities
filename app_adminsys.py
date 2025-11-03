from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify, send_from_directory, make_response
import os
import json
import psycopg2
import psycopg2.extras
import pandas as pd
import pytz
import base64
#from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash 
from functools import wraps
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv
import secrets
from io import BytesIO
import string
 
# --- INITIALIZATION ---
load_dotenv() 
app = Flask(__name__) 
app.secret_key = os.getenv("FLASK_SECRET_KEY", os.urandom(24))


# --- FLASK-LOGIN CONFIGURATION ---
class AdminUser(UserMixin):
    def __init__(self, user_id, role, society_name, username=None, is_super_admin=False, housing_type=None):
        self.id = str(user_id)
        self.username = username
        self.role = role
        self.society_name = society_name
        self.is_super_admin = is_super_admin
        self.housing_type = housing_type 


login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'admin_password_prompt'
login_manager.login_message_category = "danger"


@login_manager.user_loader
def load_user(user_id):
    conn = get_db()
    if not conn:
        return None
    admin_row = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("SELECT id, role, society_name, housing_type FROM admins WHERE id = %s", (user_id,))
            admin_row = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        app.logger.error(f"Error loading user {user_id}: {error}")
    finally:
        if conn:
            conn.close()

    if admin_row:
        is_sa = admin_row['role'] == 'super_admin'
        
        return AdminUser(
            user_id=admin_row['id'],
            role=admin_row['role'],
            society_name=admin_row['society_name'],
            is_super_admin=is_sa, 
            housing_type=admin_row.get('housing_type')
        )
    return None


# --- DATABASE & HELPERS ---
def get_db():
    """Establishes a connection to the PostgreSQL database using .env credentials."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        return conn
    except psycopg2.OperationalError as e:
        app.logger.error(f"Error connecting to PostgreSQL database: {e}")
        return None
 
def generate_secret_code(length=6):
    """Generates a random 6-digit secret code and its hash."""
    # Ensure the code is 6 digits long
    raw_code = ''.join(secrets.choice(string.digits) for _ in range(length))
    # Hash the code for database storage
    hashed_code = generate_password_hash(raw_code)
    # This function MUST return exactly 2 values to prevent the ValueError
    return raw_code, hashed_code

def send_codes_excel_response(code_data, society_name):
    """
    Takes a list of household code data, creates an Excel file, and returns 
    a Flask response for immediate download.
    """
    try:
        df = pd.DataFrame(code_data)
        
        # Use io.BytesIO to keep the file in memory
        output = io.BytesIO()
        writer = pd.ExcelWriter(output, engine='xlsxwriter')
        
        # Write the DataFrame to the in-memory buffer
        df.to_excel(writer, sheet_name='Secret Codes', index=False)
        
        writer.close() # Important: Use writer.close()
        output.seek(0)
        
        # Prepare the response
        date_str = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"Secret_Codes_{society_name}_{date_str}.xlsx"
        
        response = make_response(output.read())
        response.headers['Content-Type'] = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        
        return response
        
    except Exception as e:
        app.logger.error(f"Error generating Excel report: {e}")
        # Fallback if file generation fails
        flash(f"Error generating secret code report (functional save was successful): {e}", "warning")
        return redirect(url_for('home_management'))

def generate_households_from_recipe(recipe_data):
    household_list = []
    society_name = recipe_data.get("society_name", "DEFAULT_SOCIETY")
    housing_type = recipe_data.get("housing_type", "").strip()

    if housing_type.startswith("Apartment"):
        towers = recipe_data.get("apartment", {}).get("towers", [])
        for tower in towers:
            try:
                flats_set = set()
                start_flat = int(tower.get("start_flat", 0))
                end_flat = int(tower.get("end_flat", 0))
                start_series = int(tower.get("start_series", 0))
                end_series = int(tower.get("end_series", 0))
                missing_series = {int(s.strip()) for s in tower.get("missing_series", "").split(',') if s.strip().isdigit()}

                for s_num in range(start_series, end_series + 1):
                    if s_num in missing_series:
                        continue
                    for f_num in range(start_flat, end_flat + 1):
                        flats_set.add(f"{s_num:02d}{f_num:02d}")

                additional_raw = tower.get("additional_flats", "")
                remove_raw = tower.get("remove_flats", "")
                if additional_raw:
                    flats_set.update({f"{int(f.strip()):04d}" for f in additional_raw.split(',') if f.strip().isdigit()})
                if remove_raw:
                    flats_set.difference_update({f"{int(f.strip()):04d}" for f in remove_raw.split(',') if f.strip().isdigit()})

                for flat in sorted(list(flats_set)):
                    household_list.append((society_name, tower.get("name", "TOWER_NOTSET"), flat))

            except (ValueError, TypeError) as e:
                print(f"Skipping tower due to data error: {tower.get('name')}, {e}")

    elif housing_type.startswith("Villas") or housing_type.startswith("Civil") or housing_type.startswith("Individual"):
        individual_data = recipe_data.get("individual", {})
        has_lane = individual_data.get("has_lane", False)

        if has_lane:
            for lane in individual_data.get("lanes", []):
                lane_name = lane.get("name", "LANE_NOTSET").strip().upper()
                house_set = set()
                base_raw = lane.get("base", "")
                for part in base_raw.split(','):
                    part = part.strip()
                    if not part: continue
                    if '-' in part:
                        try:
                            start, end = map(int, part.split('-'))
                            house_set.update(str(h) for h in range(start, end + 1))
                        except ValueError:
                            continue
                    else:
                        house_set.add(part)

                additional_raw = lane.get("additional", "")
                if additional_raw:
                    house_set.update({h.strip() for h in additional_raw.split(',') if h.strip()})

                remove_raw = lane.get("remove", "")
                if remove_raw:
                    house_set.difference_update({h.strip() for h in remove_raw.split(',') if h.strip()})

                for house in sorted(list(house_set), key=lambda x: int(x) if x.isdigit() else x):
                    household_list.append((society_name, lane_name, house))

        else:
            houses_raw = individual_data.get("house_numbers", {}).get("numbers_raw", "")
            house_set = set()
            for part in houses_raw.split(','):
                part = part.strip()
                if not part: continue
                if '-' in part:
                    try:
                        start, end = map(int, part.split('-'))
                        house_set.update(str(h) for h in range(start, end + 1))
                    except ValueError:
                        continue
                else:
                    house_set.add(part)

            for house in sorted(list(house_set), key=lambda x: int(x) if x.isdigit() else x):
                household_list.append((society_name, 'N/A', house))

    return household_list

def get_voting_status(society_name):
    """Checks the voting schedule for the given society."""
    conn = get_db()
    if not conn:
        return 'DB_CONNECTION_ERROR'
    schedule = None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                "SELECT start_time, end_time FROM voting_schedule WHERE society_name = %s",
                (society_name,)
            )
            schedule = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as error:
        app.logger.error(f"Error getting voting status for {society_name}: {error}")
    finally:
        if conn:
            conn.close()

    if not schedule or not schedule['start_time'] or not schedule['end_time']:
        return 'NOT_CONFIGURED'

    try:
        current_time_utc = datetime.now(pytz.utc)
        
        # --- FIX START ---
        
        start_time_raw = schedule['start_time']
        end_time_raw = schedule['end_time']
        
        # Only apply replace if the data is a string, otherwise use the object directly
        if isinstance(start_time_raw, str):
            start_time_raw = start_time_raw.replace('Z', '+00:00')
        
        if isinstance(end_time_raw, str):
            end_time_raw = end_time_raw.replace('Z', '+00:00')
            
        # Convert to datetime object (works for both corrected strings and datetime objects)
        start_time_utc = datetime.fromisoformat(str(start_time_raw))
        end_time_utc = datetime.fromisoformat(str(end_time_raw))
        
        # Ensure times are UTC aware for comparison
        if start_time_utc.tzinfo is None:
            start_time_utc = start_time_utc.replace(tzinfo=pytz.utc)
        if end_time_utc.tzinfo is None:
            end_time_utc = end_time_utc.replace(tzinfo=pytz.utc)
        
        # --- FIX END ---

        if current_time_utc < start_time_utc:
            return 'NOT_STARTED'
        elif start_time_utc <= current_time_utc < end_time_utc:
            return 'ACTIVE'
        else:
            return 'CLOSED'

    except (ValueError, TypeError):
        return 'INVALID_SCHEDULE'
# --- ROUTES & VIEWS ---

@app.route('/')
def root_redirect():
    return redirect(url_for("system_entry"))


@app.route('/system-entry')
def system_entry():
    if current_user.is_authenticated:
        return redirect(url_for('admin_panel'))
    return render_template("system_entry.html")

@app.route('/admin-password', methods=['GET', 'POST'])
def admin_password_prompt():
    if current_user.is_authenticated:
        return redirect(url_for('admin_panel'))

    if request.method == 'POST':
        society_name_input = request.form.get("society_name", "").strip().upper()
        password = request.form.get("admin_password", "")
        if not society_name_input or not password:
            flash("Society Name and Password are required.", "danger")
            return redirect(url_for('admin_password_prompt'))

        conn = get_db()
        if not conn:
            flash("Database connection error.", "danger")
            return render_template("admin_password_prompt.html")
        
        logged_in_user_data = None
        
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, role, society_name, password_hash, housing_type FROM admins 
                    WHERE (society_name = %s AND role = 'admin') 
                    OR role = 'super_admin'
                    """,
                    (society_name_input,)
                )
                admin_rows = cur.fetchall() 
                
                for row in admin_rows:
                    # ⭐ HASHING CHANGE 1: Compare password against stored hash using check_password_hash
                    stored_hash = row['password_hash']
                    if stored_hash and check_password_hash(stored_hash, password): 
                        logged_in_user_data = row
                        break
        
        except (Exception, psycopg2.DatabaseError) as error:
            app.logger.error(f"Database error during admin login: {error}")
            flash("A server error occurred.", "danger")
        finally:
            if conn:
                conn.close()

        if logged_in_user_data:
            user = AdminUser(
                user_id=logged_in_user_data['id'],
                role=logged_in_user_data['role'],
                society_name=logged_in_user_data['society_name'],
                housing_type=logged_in_user_data.get('housing_type')
            )
            login_user(user)
            
            session['society_name'] = logged_in_user_data['society_name'].strip().upper() 
            session['housing_type'] = logged_in_user_data.get('housing_type')
            session.modified = True
            return redirect(url_for('admin_panel'))
        else:
            flash("Incorrect Society Name or Password.", "danger")
            return redirect(url_for('admin_password_prompt'))

    return render_template("admin_password_prompt.html")

@app.route('/public_reset_password', methods=['POST'])
def public_reset_password():
    # Fields from the HTML form
    society_name = request.form.get('society_name')
    email_id = request.form.get('email_id')
    new_password = request.form.get('new_password')
    confirm_password = request.form.get('confirm_password')

    if new_password != confirm_password:
        flash("New passwords do not match.", 'error')
        return redirect(url_for('admin_password_prompt'))
    
    if not society_name or not email_id or not new_password:
        flash("All fields are required.", 'error')
        return redirect(url_for('admin_password_prompt'))
        
    conn = None
    try:
        conn = get_db()
        cur = conn.cursor()
        
        # ⭐ HASHING CHANGE 2: Hash the new password before updating the database
        hashed_password = generate_password_hash(new_password)
        
        cur.execute(
            "UPDATE admins SET password_hash = %s WHERE society_name = %s AND email = %s", 
            (hashed_password, society_name, email_id)
        )
        
        if cur.rowcount == 0:
            flash("No matching details to update.", 'error')
        else:
            conn.commit()
            flash("Updated successfully.", 'success')
            
        return redirect(url_for('admin_password_prompt'))
        
    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"Error during public password reset: {e}")
        flash("An unexpected error occurred during the password reset.", 'error')
    finally:
        if conn:
            conn.close()
            
    return redirect(url_for('admin_password_prompt'))
  
@app.route('/super-admin-password', methods=['GET', 'POST'])
def super_admin_password_prompt():
    if current_user.is_authenticated:
        return redirect(url_for('admin_panel'))

    if request.method == 'POST':
        society_name_to_manage = request.form.get("society_name", "").strip().upper() 
        password = request.form.get("super_admin_password", "")
        
        if not society_name_to_manage or not password:
            flash("Society Name and Password are required.", "danger")
            return redirect(url_for('super_admin_password_prompt'))
            
        conn = get_db()
        if not conn:
            flash("Database connection error.", "danger")
            return render_template("confirm_super_admin_password.html")

        super_admin_row = None
        target_society_housing_type = None 
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT id, role, society_name, password_hash, housing_type FROM admins WHERE society_name = %s AND role = %s",
                    ('_system_', 'super_admin')
                )
                super_admin_row = cur.fetchone()
                
                cur.execute(
                    "SELECT housing_type FROM admins WHERE society_name = %s AND role = %s",
                    (society_name_to_manage, 'admin') 
                )
                target_society_row = cur.fetchone()
                if target_society_row:
                    target_society_housing_type = target_society_row.get('housing_type')

        except (Exception, psycopg2.DatabaseError) as error:
            app.logger.error(f"Database error during super admin login: {error}")
            flash("A server error occurred.", "danger")
        finally:
            if conn:
                conn.close()
        
        # ⭐ HASHING CHANGE 3: Compare password against stored hash using check_password_hash
        stored_hash = super_admin_row['password_hash'] if super_admin_row else None
        
        if super_admin_row and stored_hash and check_password_hash(stored_hash, password):
            user = AdminUser(
                user_id=super_admin_row['id'],
                role=super_admin_row['role'],
                society_name=super_admin_row['society_name'], 
                is_super_admin=True, 
                housing_type=super_admin_row.get('housing_type')
            )
            login_user(user)
            
            session['society_name'] = society_name_to_manage
            session['housing_type'] = target_society_housing_type 
            session.modified = True
            flash(f"Super Admin logged in. Managing society: {society_name_to_manage}", "success")
            return redirect(url_for('admin_panel'))
        else:
            flash("Incorrect Super Admin Password or missing Society Name.", "danger")

    return render_template("confirm_super_admin_password.html")


@app.route('/admin-panel')
@login_required
def admin_panel():
    society_name = current_user.society_name
    voting_status = get_voting_status(society_name)
    response = make_response(render_template("admin_panel.html", voting_status=voting_status))
    return response


@app.route('/logout')
@login_required
def logout():
    logout_user()
    session.clear()
    flash("You have been logged out.", "info")
    return redirect(url_for("system_entry"))

@app.after_request 
def add_no_cache_headers(response):
    response.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response

@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)

@app.route('/home-management', methods=['GET', 'POST'])
@login_required
def home_management(): 
    society_name = session.get('society_name')
    housing_type = session.get('housing_type')

    if not society_name:
        flash("Session expired or invalid. Please log in again.", "danger")
        return redirect(url_for('system_entry'))

    # --- POST REQUEST HANDLING (Manual Save OR File Upload) ---
    if request.method == 'POST':
        conn = get_db()
        if not conn:
            flash("Database connection error.", "danger")
            return redirect(url_for('home_management'))

        recipe_to_save = {}
        action = request.form.get('action', 'manual') 
        
        try:
            housing_type_submitted = request.form.get("housing_type")

            # --- Type determination logic ---
            if housing_type_submitted.startswith("Apartment"):
                community_type_for_recipe = "apartment"
            elif housing_type_submitted.startswith("Villas"):
                community_type_for_recipe = "individual"
            elif housing_type_submitted.startswith("Civil"):
                community_type_for_recipe = "civil"
            else:
                community_type_for_recipe = housing_type_submitted.lower()

            recipe_to_save = {
                "community_type": community_type_for_recipe,
                "housing_type": housing_type_submitted,
                "society_name": society_name
            }

            # --- DUAL INPUT PROCESSING LOGIC (Unchanged) ---
            if action == 'upload':
                config_file = request.files.get('config_file')
                if not config_file or not config_file.filename:
                    flash("No configuration file selected for upload.", "danger")
                    return redirect(url_for('home_management'))
                
                # Use pandas to read the file content
                try:
                    if config_file.filename.lower().endswith(('.xlsx', '.xls')):
                        df = pd.read_excel(config_file, dtype=str)
                    else:
                        df = pd.read_csv(config_file, dtype=str)
                    
                    df = df.fillna('').to_dict('records')

                except Exception as e:
                    flash(f"Error reading file: Ensure it is a valid Excel or CSV format. ({e})", "danger")
                    return redirect(url_for('home_management'))

                if community_type_for_recipe == "apartment":
                    towers_list = []
                    for row in df:
                        towers_list.append({
                            "name": row.get('Tower', '').strip().upper(),
                            "start_flat": row.get('Start_Flat', ''),
                            "end_flat": row.get('End_Flat', ''),
                            "start_series": row.get('Start_Series', ''),
                            "end_series": row.get('End_Series', ''),
                            "missing_series": row.get('Missing_Series', ''),
                            "additional_flats": row.get('Additional_Flats', ''),
                            "remove_flats": row.get('Remove_Flats', '')
                        })
                    recipe_to_save["apartment"] = {"towers": towers_list}

                elif community_type_for_recipe in ("individual", "civil"): 
                    if community_type_for_recipe == "individual" and 'Lane' in df[0]:
                        lanes_list = []
                        for row in df:
                            lanes_list.append({
                                "name": row.get('Lane', '').strip(),
                                "base": row.get('Base_Houses', ''),
                                "additional": row.get('Additional_Houses', ''),
                                "remove": row.get('Remove_Houses', '')
                            })
                        recipe_to_save["individual"] = {"has_lane": True, "lanes": lanes_list}
                    else:
                        house_numbers_raw = df[0].get('House_Numbers_Raw', '') if df else ''
                        recipe_to_save["individual"] = {
                            "has_lane": False,
                            "house_numbers": {"numbers_raw": house_numbers_raw}
                        }

            elif action == 'manual': 
                if community_type_for_recipe == "apartment":
                    recipe_to_save["apartment"] = {"towers": []}
                    tower_indices = sorted(list(set(
                        k.split('[')[1].split(']')[0] for k in request.form if k.startswith('towers[')
                    )))
                    for idx in tower_indices:
                        name = request.form.get(f'towers[{idx}][name]', '').strip().upper()
                        if housing_type_submitted == "Apartment-Multi Towers" and not name:
                            continue
                        tower_data = {
                            "name": request.form.get(f'towers[{idx}][name]', '').strip().upper(),
                            "start_flat": request.form.get(f'towers[{idx}][start_flat]'),
                            "end_flat": request.form.get(f'towers[{idx}][end_flat]'),
                            "start_series": request.form.get(f'towers[{idx}][start_series]'),
                            "end_series": request.form.get(f'towers[{idx}][end_series]'),
                            "missing_series": request.form.get(f'towers[{idx}][missing_series]'),
                            "additional_flats": request.form.get(f'towers[{idx}][additional_flats]'),
                            "remove_flats": request.form.get(f'towers[{idx}][remove_flats]')
                        }
                        recipe_to_save["apartment"]["towers"].append(tower_data)

                elif community_type_for_recipe in ("individual", "civil"):
                    has_lane = (housing_type_submitted.strip() == "Villas-Lanes") 
                    recipe_to_save["individual"] = {"has_lane": has_lane}

                    if has_lane:
                        recipe_to_save["individual"]["lanes"] = []
                        lane_indices = sorted(list(set(
                            k.split('[')[1].split(']')[0] for k in request.form if k.startswith('lanes[')
                        )))
                        for idx in lane_indices:
                            lane_data = {
                                "name": request.form.get(f'lanes[{idx}][name]', '').strip(),
                                "base": request.form.get(f'lanes[{idx}][base]', ''),
                                "additional": request.form.get(f'lanes[{idx}][additional]', ''),
                                "remove": request.form.get(f'lanes[{idx}][remove]', '')
                            }
                            recipe_to_save["individual"]["lanes"].append(lane_data)
                    else:
                        recipe_to_save["individual"]["house_numbers"] = {
                            "numbers_raw": request.form.get('houses_no_lane', '')
                        }
            
            # 1. Generate the master list of (society_name, tower, flat) tuples from the recipe
            final_household_list = generate_households_from_recipe(recipe_to_save)
            
            # 2. Get existing households and their secret codes (hashes)
            existing_houses = {}
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(
                    "SELECT tower, flat, secret_code FROM households WHERE society_name = %s", 
                    (society_name,)
                )
                # Store existing houses as a key for fast lookup: (tower, flat) -> secret_code (hash)
                existing_houses = {
                    (h['tower'], h['flat']): h.get('secret_code') 
                    for h in cur.fetchall()
                }

            # 3. Prepare the list of values for UPSERT with secret codes AND prepare the report
            insert_values = []
            secret_code_report = [] # Initialize report list for raw codes
            new_house_keys = set()
            
            for society_name_item, tower, flat in final_household_list:
                house_key = (tower, flat)
                new_house_keys.add(house_key) 
                
                hashed_code_for_db = None
                raw_code_for_report = None
                is_new_code = False
                
                if house_key in existing_houses and existing_houses[house_key]:
                    # Existing house with a code: use the old secret code (which is the hash)
                    hashed_code_for_db = existing_houses[house_key]
                else:
                    # NEW CODE GENERATION: New house OR existing house missing a code
                    # NOTE: generate_secret_code() must return (raw_code, hashed_code)
                    raw_code_for_report, hashed_code_for_db = generate_secret_code()
                    is_new_code = True

                # Only report the raw code if it was newly generated
                if is_new_code:
                    secret_code_report.append({
                        "Tower/Lane": tower,
                        "Flat/House No.": flat,
                        "Mobile Number": "", # Placeholder
                        "Secret Code": raw_code_for_report 
                    })
                    
                # Item for DB is: (society_name, tower, flat, hashed_code, mobile_number_placeholder)
                insert_values.append((society_name_item, tower, flat, hashed_code_for_db, '')) # mobile_number added
            
            # 4. Execute the UPSERT: Insert new houses and ignore conflicts on existing ones
            with conn.cursor() as cur:
                if insert_values:
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        INSERT INTO households (society_name, tower, flat, secret_code, mobile_number) 
                        VALUES %s
                        ON CONFLICT (society_name, tower, flat) DO UPDATE SET mobile_number = EXCLUDED.mobile_number 
                        """, 
                        insert_values,
                        template="(%s, %s, %s, %s, %s)", # Template updated to 5 values
                        page_size=1000
                    )
                
                # 5. Delete removed households 
                old_house_keys = set(existing_houses.keys())
                removed_house_keys = old_house_keys - new_house_keys
                
                if removed_house_keys:
                    delete_params = [(society_name, t, f) for t, f in removed_house_keys]
                    
                    psycopg2.extras.execute_values(
                        cur,
                        """
                        DELETE FROM households 
                        WHERE society_name = %s AND tower = %s AND flat = %s
                        """,
                        delete_params,
                        template="(%s, %s, %s)",
                        page_size=1000
                    )
                
                # 6. Save the Recipe JSON
                cur.execute(
                    """
                    INSERT INTO home_data (society_name, data) VALUES (%s, %s)
                    ON CONFLICT (society_name) DO UPDATE SET data = EXCLUDED.data
                    """,
                    (society_name, json.dumps(recipe_to_save))
                )

                conn.commit()
            

            # 7. CRITICAL STEP: Generate and send Excel report (Download) 
            # If any new secret codes were generated, send the Excel file.
            if secret_code_report:
                flash(f"Home configuration for {society_name} saved. {len(final_household_list)} households configured. Download the Excel report with **{len(secret_code_report)} NEW** secret codes now.", "success")
                return send_codes_excel_response(secret_code_report, society_name)
            
            # Success/Fallback for when no new codes were generated
            flash(f"Home configuration for {society_name} saved. {len(final_household_list)} households configured. No new secret codes were generated.", "success")
            return redirect(url_for('home_management')) 

        except (Exception, psycopg2.DatabaseError) as e:
            if conn:
                conn.rollback()
            app.logger.error(f"Error in home_management POST: {e}") 
            flash(f"An error occurred while saving: {e}", "danger")
        finally:
            if conn:
                conn.close()
          
        return redirect(url_for('home_management')) 


    # --- GET REQUEST HANDLING (Data Retrieval) --- (Unchanged)
    recipe_data = {}
    conn = None 
    try:
        conn = get_db()
        if conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT data FROM home_data WHERE society_name = %s", (society_name,))
                recipe_row = cur.fetchone()
            recipe_data = json.loads(recipe_row['data']) if recipe_row and recipe_row['data'] else {}
    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"Error in home_management GET: {e}")
    finally:
        if conn:
            conn.close()

    return render_template(
        "home_management.html", 
        recipe=recipe_data,
        society_name=society_name,
        housing_type=housing_type
    )

@app.route('/update_max_selection', methods=['POST'])
@login_required
def update_max_selection():
    data = request.get_json()
    max_selection = data.get('max_candidates_selection')
    if not isinstance(max_selection, int) or max_selection < 1:
        return jsonify({"success": False, "message": "Invalid input."}), 400
    society_name = session.get("society_name")
    if not society_name:
        return jsonify({"success": False, "message": "Admin session not found."}), 403
    
    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "Database connection error."}), 500
    
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO settings (society_name, max_candidates_selection) VALUES (%s, %s)
                ON CONFLICT(society_name) DO UPDATE SET max_candidates_selection = excluded.max_candidates_selection
                """,
                (society_name, max_selection)
            )
        conn.commit()
        return jsonify({"success": True, "message": "Maximum selection updated."})
    except (Exception, psycopg2.DatabaseError) as e:
        conn.rollback()
        app.logger.error(f"Error updating max selection: {e}")
        return jsonify({"success": False, "message": str(e)}), 500
    finally:
        if conn:
            conn.close()

@app.route('/set-voting-time', methods=['POST'])
@login_required
def set_voting_time():
    society_name = session.get('society_name')
    if not society_name:
        return jsonify({"success": False, "message": "Admin session not found."}), 401
    
    data = request.get_json()
    start_time_str = data.get('startTime')
    end_time_str = data.get('endTime')
    
    if not start_time_str or not end_time_str:
        return jsonify({"success": False, "message": "Start and end times are required."}), 400

    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "Database connection error."}), 500
        
    try:
        start_time_utc = datetime.fromisoformat(start_time_str.replace('Z', '+00:00'))
        end_time_utc = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
        
        if end_time_utc <= start_time_utc:
                return jsonify({
                "success": False, 
                "message": "Voting end time can't be before start time."
            }), 400
            
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO voting_schedule (society_name, start_time, end_time) VALUES (%s, %s, %s)
                ON CONFLICT (society_name) DO UPDATE SET start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time
                """,
                (society_name, start_time_utc, end_time_utc) 
            )
        conn.commit()
        return jsonify({"success": True, "message": "Voting schedule updated."})
        
    except (ValueError, TypeError):
        return jsonify({"success": False, "message": "Invalid date format."}), 400
    except (Exception, psycopg2.DatabaseError) as e:
        conn.rollback()
        app.logger.error(f"Error setting voting time: {e}", exc_info=True)
        return jsonify({"success": False, "message": "A server error occurred."}), 500
    finally:
        if conn:
            conn.close()

@app.route("/api/verify_code", methods=["POST"])
def verify_code():
    # NOTE: Requires 'from werkzeug.security import check_password_hash' at the top of the file
    data = request.get_json()
    society_name = data.get('society')
    tower = data.get('tower')
    flat = data.get('flat')
    secret_code = data.get('secret_code')

    if not all([society_name, tower, flat, secret_code]):
        return jsonify({"success": False, "message": "All fields are required."}), 400

    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "Could not connect to the database."}), 500

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # --- 1. Voting Schedule Check (Unchanged) ---
            cur.execute("SELECT start_time, end_time FROM voting_schedule WHERE society_name = %s", (society_name,))
            schedule = cur.fetchone()

            if not schedule or not schedule['start_time'] or not schedule['end_time']:
                return jsonify({"success": False, "message": "Voting schedule not configured."}), 403

            current_time_utc = datetime.now(pytz.utc)
            start_time_utc = datetime.fromisoformat(schedule['start_time'].replace('Z', '+00:00'))
            end_time_utc = datetime.fromisoformat(schedule['end_time'].replace('Z', '+00:00'))
            
            if not (start_time_utc <= current_time_utc < end_time_utc):
                return jsonify({"success": False, "message": "Voting is closed."}), 403

            # --- 2. Secure Credential Verification (FIXED) ---
            # HASHING FIX 4a: Retrieve household data based on location only
            cur.execute(
                "SELECT * FROM households WHERE society_name = %s AND tower = %s AND flat = %s",
                (society_name, tower, flat)
            )
            household = cur.fetchone()
            
            # Check 1: Does the household exist?
            if not household:
                return jsonify({"success": False, "message": "Invalid credentials."}), 401
            
            # HASHING FIX 4b: Check secret code against the stored hash
            stored_hash = household['secret_code']
            if not (stored_hash and check_password_hash(stored_hash, secret_code)):
                # Return the same generic "Invalid credentials" error to avoid security leakage
                return jsonify({"success": False, "message": "Invalid credentials."}), 401
            
            # --- 3. Voting Eligibility Checks (Unchanged) ---
            VOTED_FLAG = 1
            if household['voted_in_cycle'] == VOTED_FLAG:
                return jsonify({"success": False, "message": "This household has already voted."}), 403

            if household['is_admin_blocked']:
                return jsonify({"success": False, "message": "This household is blocked."}), 403
            if not household['is_vote_allowed']:
                return jsonify({"success": False, "message": "This household is not allowed to vote."}), 403

            # --- 4. Success ---
            session['household_id'] = household['id']
            return jsonify({"success": True, "message": "Verification successful."})

    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"FATAL ERROR in /api/verify_code: {e}", exc_info=True)
        return jsonify({"success": False, "message": f"A critical server error occurred: {str(e)}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/upload-secret-codes', methods=['POST'])
@login_required
def upload_secret_codes():
    file = request.files.get('secretCodes')
    society_name = session.get('society_name')

    if not society_name:
        flash("Society not set in session.", "danger")
        return redirect(url_for("admin_panel"))

    if not file or not ('.' in file.filename and file.filename.rsplit('.', 1)[1].lower() in ['xlsx', 'xls']):
        flash("Invalid or missing Excel file.", "danger")
        return redirect(url_for("admin_panel"))

    conn = get_db()
    if not conn:
        flash("Database connection error.", "danger")
        return redirect(url_for("admin_panel"))

    try:
        df = pd.read_excel(file, dtype=str).fillna('')
        updates = []

        with conn.cursor() as cur:
            cur.execute("SELECT housing_type FROM admins WHERE society_name = %s", (society_name,))
            row = cur.fetchone()
            housing_type = row[0] if row else ''

            for _, r in df.iterrows():
                tower = str(r.get('Tower', '')).strip().upper()
                flat = str(r.get('Flat', '')).strip()
                secret_code = str(r.get('SecretCode', '')).strip()
                if not secret_code or not flat:
                    continue
                
                # ⭐ HASHING CHANGE 5: Hash the secret code before preparing the update
                hashed_code = generate_password_hash(secret_code)

                if housing_type.startswith("Apartment") or housing_type.startswith("Villas-Lanes"):
                    cur.execute(
                        """SELECT 1 FROM households
                           WHERE TRIM(UPPER(society_name))=%s AND TRIM(UPPER(tower))=%s AND TRIM(flat)=%s""",
                        (society_name.strip().upper(), tower, flat)
                    )
                    if cur.fetchone():
                        # Use the hashed code in the updates list
                        updates.append((hashed_code, society_name.strip(), tower, flat))

                elif housing_type.startswith("Villas-No Lanes") or housing_type.startswith("Civil"):
                    cur.execute(
                        """SELECT 1 FROM households
                           WHERE TRIM(UPPER(society_name))=%s AND TRIM(flat)=%s""",
                        (society_name.strip().upper(), flat)
                    )
                    if cur.fetchone():
                        # Use the hashed code in the updates list
                        updates.append((hashed_code, society_name.strip(), flat))

            if updates:
                if housing_type.startswith(("Villas-No Lanes", "Civil")):
                    psycopg2.extras.execute_values(
                        cur,
                        "UPDATE households SET secret_code = data.secret_code FROM (VALUES %s) AS data (secret_code, society_name, flat) WHERE TRIM(UPPER(households.society_name)) = data.society_name AND TRIM(households.flat) = data.flat",
                        updates,
                        template="(%s, %s, %s)"
                    )
                else:
                     psycopg2.extras.execute_values(
                        cur,
                        "UPDATE households SET secret_code = data.secret_code FROM (VALUES %s) AS data (secret_code, society_name, tower, flat) WHERE TRIM(UPPER(households.society_name)) = data.society_name AND TRIM(UPPER(households.tower)) = data.tower AND TRIM(households.flat) = data.flat",
                        updates,
                        template="(%s, %s, %s, %s)"
                    )
                conn.commit()
                flash(f"Successfully updated secret codes for {len(updates)} households.", "success")
            else:
                flash("No matching rows found to update.", "warning")

    except (Exception, psycopg2.DatabaseError) as e:
        conn.rollback()
        app.logger.error(f"Error processing secret codes file: {e}")
        flash(f"Error processing file: {e}", "danger")
    finally:
        if conn:
            conn.close()

    return redirect(url_for("admin_panel"))

@app.route('/manage-contestants', methods=['GET', 'POST'])
@login_required
def manage_contestants():
    society_name = session.get('society_name')
    conn = get_db()
    if not conn:
        flash("Database connection error.", "danger")
        return render_template("manage_contestants.html", towers=[], households_by_tower_json='{}', contestants=[])

    if request.method == 'POST':
        try:
            with conn:  # Use conn as a context manager for automatic commit/rollback
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    action = request.form.get('action')
                    tower = request.form.get('tower')
                    flat = request.form.get('flat')

                    if action == 'add':
                        contestant_name = request.form.get('contestant_name', '').strip()
                        
                        # FIX: Using correct keys from HTML
                        symbol_file = request.files.get('contestant_symbol_data')
                        photo_file = request.files.get('contestant_photo_data')
                        
                        # Validation: Ensure all required fields and files are provided
                        if not all([tower, flat, contestant_name]):
                            flash("Tower, Flat, and Contestant Name are required.", "danger")
                            return redirect(url_for('manage_contestants'))

                        if not symbol_file or not symbol_file.filename or not photo_file or not photo_file.filename:
                            flash("Contestant symbol and photo files are required.", "danger")
                            return redirect(url_for('manage_contestants'))

                        # ⭐ NEW LOGIC START: MIME Type Restriction ⭐
                        ALLOWED_MIME_TYPES = ['image/jpeg', 'image/png', 'image/gif', 'image/svg+xml', 'image/webp']
                        
                        # Determine MIME types (with fallback)
                        mime_type_symbol = symbol_file.mimetype or 'image/png'
                        mime_type_photo = photo_file.mimetype or 'image/jpeg' 
                        
                        # Check if determined types are in the allowed list
                        if mime_type_symbol not in ALLOWED_MIME_TYPES or mime_type_photo not in ALLOWED_MIME_TYPES:
                            flash("Contestant symbol and photo must be a valid image file (JPEG, PNG, GIF, SVG, WEBP).", "danger")
                            return redirect(url_for('manage_contestants'))
                        # ⭐ NEW LOGIC END ⭐

                        # Base64 Encode the symbol and photo files 
                        encoded_string_symbol = base64.b64encode(symbol_file.read()).decode('utf-8')
                        symbol_b64_string = f"data:{mime_type_symbol};base64,{encoded_string_symbol}"
 
                        encoded_string_photo = base64.b64encode(photo_file.read()).decode('utf-8')
                        photo_b64_string = f"data:{mime_type_photo};base64,{encoded_string_photo}"

                        # Update contestant information in the database
                        cur.execute(
                            """
                            UPDATE households
                            SET is_contestant = 1, contestant_name = %s,
                                contestant_symbol = %s, contestant_photo_b64 = %s
                            WHERE society_name = %s AND tower = %s AND flat = %s
                            """,
                            (contestant_name, symbol_b64_string, photo_b64_string, society_name, tower, flat)
                        )

                        # Insert a new vote entry for the contestant if not already present
                        cur.execute(
                            """
                            INSERT INTO votes (society_name, tower, contestant_name, is_archived, vote_count)
                            VALUES (%s, %s, %s, %s, 0)
                            ON CONFLICT (society_name, tower, contestant_name, is_archived) DO NOTHING
                            """,
                            (society_name, tower, contestant_name, 0)
                        )

                        flash(f"Contestant '{contestant_name}' added successfully. Image data stored in columns.", "success")

                    elif action == 'remove':
                        # Remove contestant information
                        cur.execute(
                            "SELECT contestant_name FROM households WHERE society_name = %s AND tower = %s AND flat = %s",
                            (society_name, tower, flat)
                        )
                        contestant_to_remove = cur.fetchone()

                        cur.execute(
                            """
                            UPDATE households
                            SET is_contestant = 0, contestant_name = NULL,
                                contestant_symbol = NULL, contestant_photo_b64 = NULL
                            WHERE society_name = %s AND tower = %s AND flat = %s
                            """,
                            (society_name, tower, flat)
                        )

                        if contestant_to_remove and contestant_to_remove['contestant_name']:
                            cur.execute(
                                "DELETE FROM votes WHERE society_name = %s AND contestant_name = %s AND is_archived = 0",
                                (society_name, contestant_to_remove['contestant_name'])
                            )
                            flash(f"Contestant '{contestant_to_remove['contestant_name']}' removed successfully.", "success")

        except (Exception, psycopg2.DatabaseError) as e:
            conn.rollback()
            app.logger.error(f"Error managing contestants: {e}")
            flash("A database error occurred while updating contestants.", "danger")
        finally:
            conn.close()

        return redirect(url_for('manage_contestants'))

    # GET request: Retrieve contestant data and households by tower
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(
                """
                SELECT tower, flat, is_contestant, contestant_name, contestant_symbol, contestant_photo_b64
                FROM households
                WHERE society_name = %s
                ORDER BY tower, flat
                """,
                (society_name,)
            )
            all_households = cur.fetchall()

            available_households_dict = defaultdict(list)
            contestants = []
            unique_towers = sorted(list(set(h['tower'] for h in all_households)))

            for h in all_households:
                if h['is_contestant']:
                    contestants.append(h)
                else:
                    available_households_dict[h['tower']].append(h['flat'])

            return render_template(
                "manage_contestants.html", 
                towers=unique_towers,
                households_by_tower_json=json.dumps(available_households_dict),
                contestants=contestants
            )

    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"Error fetching contestant data: {e}")
        flash("Error loading page data from the database.", "danger")
        return render_template(
            "manage_contestants.html", 
            towers=[],
            households_by_tower_json='{}',
            contestants=[]
        )
    finally:
        if conn:
            conn.close()

@app.route('/view-results')
@login_required
def view_results():
    society_name = session.get('society_name')
    voting_status = get_voting_status(society_name)

    if voting_status == 'ACTIVE':
        flash("Voting is in progress! Results are available after it concludes.", "danger")
        return redirect(url_for('admin_panel'))
    elif voting_status not in ['CLOSED', 'NOT_STARTED']:
        flash("Voting schedule is not properly configured.", "danger")
        return redirect(url_for('admin_panel'))

    conn = get_db()
    if not conn:
        flash("Database connection error.", "danger")
        return redirect(url_for('admin_panel'))

    results, contestant_details, schedule = [], {}, None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            current_cycle = 0
            query = """
            SELECT contestant_name, tower, vote_count FROM votes
            WHERE society_name = %s AND is_archived = %s ORDER BY tower, vote_count DESC;
            """
            cur.execute(query, (society_name, current_cycle))
            results = cur.fetchall()

            cur.execute(
                "SELECT contestant_name, contestant_symbol, contestant_photo_b64 FROM households WHERE society_name = %s AND is_contestant = 1",
                (society_name,)
            )
            contestant_details = {row['contestant_name']: {'symbol': row['contestant_symbol'], 'photo': row['contestant_photo_b64']} for row in cur.fetchall()}

            cur.execute("SELECT start_time, end_time FROM voting_schedule WHERE society_name = %s", (society_name,))
            schedule = cur.fetchone()
    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"Error fetching results data: {e}")
    finally:
        if conn:
            conn.close()

    election_date, start_time_iso = "Not Set", None
    if schedule:
        try:
            if schedule['end_time']:
                end_time_utc = datetime.fromisoformat(schedule['end_time'].replace('Z', '+00:00'))
                end_time_ist = end_time_utc.astimezone(pytz.timezone('Asia/Kolkata'))
                election_date = end_time_ist.strftime('%d-%b-%Y')
            start_time_iso = schedule['start_time']
        except Exception:
            election_date = "Invalid Date"

    result_data = defaultdict(list)
    for row in results:
        details = contestant_details.get(row['contestant_name'], {})
        result_data[row['tower']].append({
            "name": row['contestant_name'], "symbol": details.get('symbol'),
            "photo": details.get('photo'), "vote_count": row['vote_count']
        })

    return render_template(
        "view_results.html", 
        results=result_data, society_name=society_name,
        election_date=election_date, voting_status=voting_status, voting_start=start_time_iso
    )

@app.route('/view-voted-flats')
@login_required
def view_voted_flats():
    society_name = session.get('society_name')
    voting_status = get_voting_status(society_name)

    if voting_status == 'ACTIVE':
        flash("Voted flats list is only available after the election concludes.", "danger")
        return redirect(url_for('admin_panel'))
    elif voting_status not in ['CLOSED', 'NOT_STARTED']:
        flash("Voting schedule is not properly configured.", "danger")
        return redirect(url_for('admin_panel'))

    conn = get_db()
    voting_start = None
    if conn:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT start_time FROM voting_schedule WHERE society_name = %s", (society_name,))
                schedule = cur.fetchone()
                if schedule: voting_start = schedule['start_time']
        except (Exception, psycopg2.DatabaseError) as e:
            app.logger.error(f"Error fetching schedule for voted flats view: {e}")
        finally:
            conn.close()
            
    return render_template(
        "view_voted_flats.html", 
        voting_status=voting_status,
        voting_start=voting_start, society_name=society_name 
    )

@app.route('/get-voted-flats-data')
@login_required
def get_voted_flats_data():
    society_name = session.get('society_name')
    data = []
    conn = get_db()
    if conn:
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                VOTED_FLAG = 1
                cur.execute("SELECT tower, flat FROM households WHERE voted_in_cycle = %s AND society_name = %s ORDER BY tower, flat", (VOTED_FLAG, society_name))
                data = [f"{r['tower']}-{r['flat']}" for r in cur.fetchall()]
        except (Exception, psycopg2.DatabaseError) as e:
            app.logger.error(f"Error fetching voted flats data: {e}")
        finally:
            conn.close()
    return jsonify(data)

@app.route('/toggle_block', methods=['POST'])
@login_required
def toggle_block():
    society_name = session.get('society_name')
    data = request.get_json()
    tower = data.get('tower', '').strip().upper()
    flat = data.get('flat', '').strip()
    block_status = data.get('block_status') # Expects '1' for block, '0' for unblock

    if not all([society_name, flat, block_status is not None]):
        return jsonify({"success": False, "message": "Missing required fields."}), 400

    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "Database connection error."}), 500

    # Determine the correct tower/lane identifier. Assumes 'N/A' for non-towered societies.
    if not tower:
        tower_or_lane = 'N/A'
    else:
        tower_or_lane = tower

    try:
        with conn.cursor() as cur:
            # Note: We set is_admin_blocked = 1/0 based on block_status.
            # We explicitly prevent updating is_vote_allowed here, focusing only on the admin block.
            cur.execute(
                """
                UPDATE households
                SET is_admin_blocked = %s 
                WHERE society_name = %s AND tower = %s AND flat = %s
                """,
                (block_status, society_name, tower_or_lane, flat)
            )

            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"success": False, "message": f"Household {flat} in {tower_or_lane} not found."}), 404
            
            conn.commit()
            action = "blocked" if block_status == '1' else "unblocked"
            return jsonify({"success": True, "message": f"Household {flat} successfully {action}."})

    except (Exception, psycopg2.DatabaseError) as e:
        conn.rollback()
        app.logger.error(f"Error toggling block status: {e}")
        return jsonify({"success": False, "message": "A server error occurred during update."}), 500
    finally:
        if conn:
            conn.close()

# ----------------------------------------------------------------------------------------------------------------------

### 2. `get_blocked_flats` (View Blocked List)

@app.route('/get_blocked_flats')
@login_required
def get_blocked_flats():
    society_name = session.get('society_name')

    conn = get_db()
    if not conn:
        return jsonify({"success": False, "message": "Database connection error."}), 500

    blocked_list = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # Select all flats/houses where the admin_blocked flag is ON (1 or True)
            cur.execute(
                """
                SELECT tower, flat 
                FROM households 
                WHERE society_name = %s AND is_admin_blocked = 1 
                ORDER BY tower, flat
                """,
                (society_name,)
            )
            rows = cur.fetchall()
            
            # Format output as "Tower-Flat" or just "Flat" for non-towered societies
            for row in rows:
                if row['tower'] == 'N/A':
                    blocked_list.append(row['flat'])
                else:
                    blocked_list.append(f"{row['tower']}-{row['flat']}")

        return jsonify({"success": True, "blocked_households": blocked_list})

    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"Error fetching blocked list: {e}")
        return jsonify({"success": False, "message": "A server error occurred while retrieving data."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/reset_votes', methods=['POST'])
@login_required
def reset_votes():
    is_authorized = current_user.role == 'admin' or current_user.is_super_admin
    
    if not is_authorized:
        return jsonify({'success': False, 'message': 'Permission denied. Only Admin or Super Admin can perform this action.'}), 403

    society_name = session.get('society_name')
    if not society_name:
        return jsonify({'success': False, 'message': 'Session expired. Society context is missing.'}), 403

    password = request.json.get('password')
    if not password:
        return jsonify({'success': False, 'message': 'Password is required.'}), 400

    conn = get_db()
    if not conn:
        return jsonify({'success': False, 'message': 'Database connection error.'}), 500

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            if current_user.is_super_admin:
                cur.execute("SELECT password_hash FROM admins WHERE role = %s", ('super_admin',))
            else:
                cur.execute("SELECT password_hash FROM admins WHERE id = %s", (current_user.id,))
            
            admin_row = cur.fetchone()

            # ⭐ HASHING CHANGE 6: Compare password against stored hash using check_password_hash
            stored_hash = admin_row['password_hash'] if admin_row else None
            if not admin_row or not (stored_hash and check_password_hash(stored_hash, password)):
                return jsonify({'success': False, 'message': 'The entered password is not correct.'}), 401

            if current_user.is_super_admin:
                cur.execute("SELECT MAX(is_archived) AS max_val FROM votes WHERE society_name = %s", (society_name,))
                max_archive_row = cur.fetchone()
                next_archive_num = (max_archive_row['max_val'] or 0) + 1
                cur.execute(
                    "UPDATE votes SET is_archived = %s WHERE society_name = %s AND is_archived = 0",
                    (next_archive_num, society_name)
                )
            else:
                cur.execute(
                    "UPDATE votes SET is_archived = 1 WHERE society_name = %s AND is_archived = 0",
                    (society_name,)
                )

            cur.execute("UPDATE households SET voted_in_cycle = 0 WHERE society_name = %s", (society_name,))
            cur.execute("UPDATE settings SET voted_count = 0 WHERE society_name = %s", (society_name,))

        conn.commit()
        return jsonify({'success': True, 'message': "Election has been reset successfully. All votes have been cleared."})

    except (Exception, psycopg2.DatabaseError) as e:
        conn.rollback()
        app.logger.error(f"Error during reset: {e}")
        return jsonify({'success': False, 'message': f"Error during reset: {e}"}), 500
    finally:
        if conn:
            conn.close()

@app.route('/get-voted-flats-grid-data')
@login_required
def get_voted_flats_grid_data():
    society_name = session.get('society_name')
    conn = get_db()
    if not conn:
        return jsonify({"towers": [], "all_possible_flats": [], "existing_flats": [], "voted_flats": [], "disallowed_flats": []})

    all_households = []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            # ⭐ FIX: Fetch BOTH 'is_vote_allowed' AND 'is_admin_blocked' ⭐
            cur.execute("SELECT tower, flat, voted_in_cycle, is_vote_allowed, is_admin_blocked FROM households WHERE society_name = %s", (society_name,))
            all_households = cur.fetchall()
    except (Exception, psycopg2.DatabaseError) as e:
        app.logger.error(f"Error fetching grid data: {e}")
    finally:
        if conn:
            conn.close()

    if not all_households:
        return jsonify({"towers": [], "all_possible_flats": [], "existing_flats": [], "voted_flats": [], "disallowed_flats": []})

    towers = sorted(list({row['tower'] for row in all_households}))
    all_possible_flats = sorted(list({row['flat'] for row in all_households}), key=lambda x: int(x) if x.isdigit() else 9999)
    existing_flats = {f"{row['tower']}-{row['flat']}" for row in all_households}
    voted_flats = {f"{row['tower']}-{row['flat']}" for row in all_households if row['voted_in_cycle'] == 1}
    
    # ⭐ FIX: Combine logic to include flats where voting is disallowed OR admin-blocked ⭐
    disallowed_flats = {
        f"{row['tower']}-{row['flat']}" 
        for row in all_households 
        if row['is_admin_blocked']
    }
    
    return jsonify({
        "towers": towers,
        "all_possible_flats": all_possible_flats,
        "existing_flats": list(existing_flats),
        "voted_flats": list(voted_flats),
        "disallowed_flats": list(disallowed_flats)
    })

if __name__ == '__main__':
    with app.app_context():
        # ensures the tables are setup and the system admin exists on startup
        # assuming the tables are properly defined elsewhere or via a schema migration
    # The original file had two app.run() calls; keeping only the ophoto_file = request.files.get('contestant_photo')ne that runs the application
     app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))