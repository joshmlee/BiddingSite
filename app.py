import os
import csv
import io
import sqlite3
from datetime import datetime
from functools import wraps

from werkzeug.security import generate_password_hash, check_password_hash
from flask import (
    Flask, render_template, request, redirect, url_for,
    session, flash, g, jsonify
)

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-change-in-production')

DATABASE = os.path.join(app.instance_path, 'auction.db')
ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD', 'admin123')

os.makedirs(app.instance_path, exist_ok=True)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
    return db


@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def init_db():
    db = get_db()
    db.executescript('''
        CREATE TABLE IF NOT EXISTS bidders (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            name             TEXT    NOT NULL,
            paddle_number    TEXT    UNIQUE NOT NULL,
            pin_hash         TEXT    NOT NULL,
            deposit_confirmed INTEGER NOT NULL DEFAULT 0,
            created_at       TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS properties (
            id                INTEGER PRIMARY KEY AUTOINCREMENT,
            address           TEXT    NOT NULL,
            parcel_number     TEXT,
            description       TEXT,
            starting_bid      REAL    NOT NULL DEFAULT 0,
            active            INTEGER NOT NULL DEFAULT 1,
            confirmed_bid_id  INTEGER REFERENCES bids(id),
            created_at        TEXT    NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS bids (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            bidder_id   INTEGER NOT NULL REFERENCES bidders(id),
            property_id INTEGER NOT NULL REFERENCES properties(id),
            amount      REAL    NOT NULL,
            timestamp   TEXT    NOT NULL DEFAULT (datetime('now'))
        );
    ''')
    # Add confirmed_bid_id to existing databases that predate this column
    try:
        db.execute('ALTER TABLE properties ADD COLUMN confirmed_bid_id INTEGER REFERENCES bids(id)')
        db.commit()
    except sqlite3.OperationalError:
        pass  # column already exists


with app.app_context():
    init_db()


# ---------------------------------------------------------------------------
# Auth decorators
# ---------------------------------------------------------------------------

def require_bidder(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'bidder_id' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated


def require_admin(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get('is_admin'):
            return redirect(url_for('admin_login'))
        return f(*args, **kwargs)
    return decorated


# ---------------------------------------------------------------------------
# Bidder routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    if 'bidder_id' in session:
        return redirect(url_for('bid'))
    return redirect(url_for('login'))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        paddle = request.form.get('paddle_number', '').strip()
        pin = request.form.get('pin', '').strip()
        db = get_db()
        bidder = db.execute(
            'SELECT * FROM bidders WHERE paddle_number = ? AND deposit_confirmed = 1',
            (paddle,)
        ).fetchone()
        if bidder and check_password_hash(bidder['pin_hash'], pin):
            session['bidder_id'] = bidder['id']
            session['bidder_name'] = bidder['name']
            session['paddle_number'] = bidder['paddle_number']
            return redirect(url_for('bid'))
        flash('Invalid paddle number or PIN, or your deposit has not been confirmed.')
    return render_template('login.html')


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))


@app.route('/bid')
@require_bidder
def bid():
    db = get_db()
    properties = db.execute(
        'SELECT * FROM properties WHERE active = 1 ORDER BY address'
    ).fetchall()
    result = []
    for prop in properties:
        top_bid = db.execute(
            '''SELECT b.amount, bi.paddle_number
               FROM bids b JOIN bidders bi ON b.bidder_id = bi.id
               WHERE b.property_id = ?
               ORDER BY b.amount DESC LIMIT 1''',
            (prop['id'],)
        ).fetchone()
        result.append({'property': prop, 'top_bid': top_bid})
    return render_template('bid.html', properties=result)


@app.route('/bid/data')
@require_bidder
def bid_data():
    """JSON endpoint polled by the bidder page to keep bid amounts live."""
    db = get_db()
    properties = db.execute(
        'SELECT id FROM properties WHERE active = 1'
    ).fetchall()
    result = {}
    for prop in properties:
        top_bid = db.execute(
            '''SELECT b.amount, bi.paddle_number
               FROM bids b JOIN bidders bi ON b.bidder_id = bi.id
               WHERE b.property_id = ?
               ORDER BY b.amount DESC LIMIT 1''',
            (prop['id'],)
        ).fetchone()
        history = db.execute(
            '''SELECT amount, timestamp
               FROM bids
               WHERE property_id = ?
               ORDER BY timestamp DESC''',
            (prop['id'],)
        ).fetchall()
        confirmed = None
        if prop['confirmed_bid_id']:
            row = db.execute(
                '''SELECT b.amount, b.bidder_id
                   FROM bids b WHERE b.id = ?''',
                (prop['confirmed_bid_id'],)
            ).fetchone()
            if row:
                confirmed = {
                    'amount': row['amount'],
                    'is_mine': row['bidder_id'] == session['bidder_id'],
                }
        result[prop['id']] = {
            'top_bid': dict(top_bid) if top_bid else None,
            'history': [dict(b) for b in history],
            'confirmed': confirmed,
        }
    return jsonify(result)


@app.route('/bid/submit', methods=['POST'])
@require_bidder
def submit_bid():
    property_id = request.form.get('property_id', type=int)
    amount = request.form.get('amount', type=float)
    if not property_id or amount is None:
        flash('Invalid bid submission.')
        return redirect(url_for('bid'))

    db = get_db()
    prop = db.execute(
        'SELECT * FROM properties WHERE id = ? AND active = 1', (property_id,)
    ).fetchone()
    if not prop:
        flash('Property not found or not currently active.')
        return redirect(url_for('bid'))

    top = db.execute(
        'SELECT MAX(amount) as max_amount FROM bids WHERE property_id = ?',
        (property_id,)
    ).fetchone()
    min_bid = max(prop['starting_bid'], top['max_amount'] or 0)

    if amount <= min_bid:
        flash(f'Bid must be greater than ${min_bid:,.2f}.')
        return redirect(url_for('bid'))

    db.execute(
        'INSERT INTO bids (bidder_id, property_id, amount) VALUES (?, ?, ?)',
        (session['bidder_id'], property_id, amount)
    )
    db.commit()
    flash(f'Bid of ${amount:,.2f} placed on {prop["address"]}!')
    return redirect(url_for('bid'))


# ---------------------------------------------------------------------------
# Relay routes
# ---------------------------------------------------------------------------

@app.route('/relay')
@require_admin
def relay():
    return render_template('relay.html')


@app.route('/relay/data')
@require_admin
def relay_data():
    db = get_db()
    properties = db.execute(
        'SELECT * FROM properties WHERE active = 1 ORDER BY address'
    ).fetchall()

    result = []
    for prop in properties:
        top_bid = db.execute(
            '''SELECT b.id, b.amount, bi.name, bi.paddle_number, b.timestamp
               FROM bids b JOIN bidders bi ON b.bidder_id = bi.id
               WHERE b.property_id = ?
               ORDER BY b.amount DESC LIMIT 1''',
            (prop['id'],)
        ).fetchone()
        confirmed = None
        if prop['confirmed_bid_id']:
            confirmed = db.execute(
                '''SELECT b.id, b.amount, bi.name, bi.paddle_number
                   FROM bids b JOIN bidders bi ON b.bidder_id = bi.id
                   WHERE b.id = ?''',
                (prop['confirmed_bid_id'],)
            ).fetchone()
        result.append({
            'id': prop['id'],
            'address': prop['address'],
            'parcel_number': prop['parcel_number'] or '',
            'starting_bid': prop['starting_bid'],
            'top_bid': dict(top_bid) if top_bid else None,
            'confirmed': dict(confirmed) if confirmed else None,
        })

    recent_activity = db.execute(
        '''SELECT b.amount, bi.name, bi.paddle_number, b.timestamp, p.address
           FROM bids b
           JOIN bidders bi ON b.bidder_id = bi.id
           JOIN properties p  ON b.property_id  = p.id
           ORDER BY b.timestamp DESC LIMIT 15'''
    ).fetchall()

    return jsonify({
        'properties': result,
        'recent_activity': [dict(r) for r in recent_activity],
        'updated_at': datetime.utcnow().strftime('%H:%M:%S UTC'),
    })


# ---------------------------------------------------------------------------
# Bid confirmation (admin action, surfaced on relay view)
# ---------------------------------------------------------------------------

@app.route('/admin/properties/<int:prop_id>/confirm', methods=['POST'])
@require_admin
def confirm_bid(prop_id):
    """Confirm the current highest bid as the winner for this property."""
    db = get_db()
    top = db.execute(
        '''SELECT b.id FROM bids b
           WHERE b.property_id = ?
           ORDER BY b.amount DESC LIMIT 1''',
        (prop_id,)
    ).fetchone()
    if top:
        db.execute('UPDATE properties SET confirmed_bid_id = ? WHERE id = ?',
                   (top['id'], prop_id))
        db.commit()
    return ('', 204)


@app.route('/admin/properties/<int:prop_id>/unconfirm', methods=['POST'])
@require_admin
def unconfirm_bid(prop_id):
    """Remove the confirmed winner for this property."""
    db = get_db()
    db.execute('UPDATE properties SET confirmed_bid_id = NULL WHERE id = ?', (prop_id,))
    db.commit()
    return ('', 204)


# ---------------------------------------------------------------------------
# Admin routes
# ---------------------------------------------------------------------------

@app.route('/admin')
@require_admin
def admin_dashboard():
    db = get_db()
    bidder_count   = db.execute('SELECT COUNT(*) FROM bidders').fetchone()[0]
    property_count = db.execute('SELECT COUNT(*) FROM properties').fetchone()[0]
    bid_count      = db.execute('SELECT COUNT(*) FROM bids').fetchone()[0]
    return render_template(
        'admin/dashboard.html',
        bidder_count=bidder_count,
        property_count=property_count,
        bid_count=bid_count,
    )


@app.route('/admin/login', methods=['GET', 'POST'])
def admin_login():
    if request.method == 'POST':
        if request.form.get('password', '') == ADMIN_PASSWORD:
            session['is_admin'] = True
            return redirect(url_for('admin_dashboard'))
        flash('Incorrect password.')
    return render_template('admin/login.html')


@app.route('/admin/logout')
def admin_logout():
    session.pop('is_admin', None)
    return redirect(url_for('admin_login'))


# -- Bidder management --

@app.route('/admin/bidders')
@require_admin
def admin_bidders():
    db = get_db()
    bidders = db.execute(
        'SELECT * FROM bidders ORDER BY CAST(paddle_number AS INTEGER), paddle_number'
    ).fetchall()
    return render_template('admin/bidders.html', bidders=bidders)


@app.route('/admin/bidders/add', methods=['POST'])
@require_admin
def admin_add_bidder():
    name   = request.form.get('name', '').strip()
    paddle = request.form.get('paddle_number', '').strip()
    pin    = request.form.get('pin', '').strip()
    confirmed = 1 if request.form.get('deposit_confirmed') else 0

    if not name or not paddle or not pin:
        flash('Name, paddle number, and PIN are all required.')
        return redirect(url_for('admin_bidders'))

    db = get_db()
    try:
        db.execute(
            'INSERT INTO bidders (name, paddle_number, pin_hash, deposit_confirmed) VALUES (?, ?, ?, ?)',
            (name, paddle, generate_password_hash(pin), confirmed)
        )
        db.commit()
        flash(f'Bidder "{name}" (Paddle #{paddle}) added successfully.')
    except sqlite3.IntegrityError:
        flash(f'Paddle number {paddle} is already in use.')
    return redirect(url_for('admin_bidders'))


@app.route('/admin/bidders/toggle/<int:bidder_id>', methods=['POST'])
@require_admin
def admin_toggle_bidder(bidder_id):
    db = get_db()
    row = db.execute('SELECT deposit_confirmed FROM bidders WHERE id = ?', (bidder_id,)).fetchone()
    if row:
        db.execute(
            'UPDATE bidders SET deposit_confirmed = ? WHERE id = ?',
            (0 if row['deposit_confirmed'] else 1, bidder_id)
        )
        db.commit()
    return redirect(url_for('admin_bidders'))


@app.route('/admin/bidders/delete/<int:bidder_id>', methods=['POST'])
@require_admin
def admin_delete_bidder(bidder_id):
    db = get_db()
    db.execute('DELETE FROM bidders WHERE id = ?', (bidder_id,))
    db.commit()
    flash('Bidder removed.')
    return redirect(url_for('admin_bidders'))


# -- Property management --

@app.route('/admin/properties')
@require_admin
def admin_properties():
    db = get_db()
    props = db.execute('SELECT * FROM properties ORDER BY address').fetchall()
    return render_template('admin/properties.html', properties=props)


@app.route('/admin/properties/upload', methods=['POST'])
@require_admin
def admin_upload_properties():
    file = request.files.get('csv_file')
    if not file or not file.filename.lower().endswith('.csv'):
        flash('Please upload a .csv file.')
        return redirect(url_for('admin_properties'))

    content = file.read().decode('utf-8-sig')  # strip BOM if present
    reader = csv.DictReader(io.StringIO(content))
    db = get_db()
    count = 0
    for row in reader:
        address = row.get('address', '').strip()
        if not address:
            continue
        db.execute(
            '''INSERT INTO properties (address, parcel_number, description, starting_bid)
               VALUES (?, ?, ?, ?)''',
            (
                address,
                row.get('parcel_number', '').strip(),
                row.get('description', '').strip(),
                float(row.get('starting_bid', 0) or 0),
            )
        )
        count += 1
    db.commit()
    flash(f'{count} properties imported from CSV.')
    return redirect(url_for('admin_properties'))


@app.route('/admin/properties/add', methods=['POST'])
@require_admin
def admin_add_property():
    address = request.form.get('address', '').strip()
    if not address:
        flash('Address is required.')
        return redirect(url_for('admin_properties'))
    db = get_db()
    db.execute(
        '''INSERT INTO properties (address, parcel_number, description, starting_bid)
           VALUES (?, ?, ?, ?)''',
        (
            address,
            request.form.get('parcel_number', '').strip(),
            request.form.get('description', '').strip(),
            float(request.form.get('starting_bid', 0) or 0),
        )
    )
    db.commit()
    flash(f'Property "{address}" added.')
    return redirect(url_for('admin_properties'))


@app.route('/admin/properties/toggle/<int:prop_id>', methods=['POST'])
@require_admin
def admin_toggle_property(prop_id):
    db = get_db()
    row = db.execute('SELECT active FROM properties WHERE id = ?', (prop_id,)).fetchone()
    if row:
        db.execute('UPDATE properties SET active = ? WHERE id = ?', (0 if row['active'] else 1, prop_id))
        db.commit()
    return redirect(url_for('admin_properties'))


@app.route('/admin/properties/delete/<int:prop_id>', methods=['POST'])
@require_admin
def admin_delete_property(prop_id):
    db = get_db()
    db.execute('DELETE FROM bids       WHERE property_id = ?', (prop_id,))
    db.execute('DELETE FROM properties WHERE id = ?',          (prop_id,))
    db.commit()
    flash('Property and all associated bids deleted.')
    return redirect(url_for('admin_properties'))


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
