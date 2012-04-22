import csv
import shutil
import os
import sqlite3
import datetime
import types
import xlsx

class AttendanceDB:
	''' Base class for the attendance database. '''
	db0 = os.path.join(os.getcwd(), 'gc-attendance.sqlite')
	
	def __init__(self, db=db0):
		self.db = db
	
	def con_cursor(self):
		''' Connect to the DB, enable foreign keys, set autocommit mode,  
		and return a (connection, cursor) pair. '''
		con = sqlite3.connect(self.db)
		con.isolation_level = None
		con.execute('PRAGMA foreign_keys = ON')
		cur = con.cursor()
		return (con, cur)
	
	def createTables(self):
		''' Create the database tables. '''
		try:
			(con, cur) = self.con_cursor()
			cur.execute('''CREATE TABLE IF NOT EXISTS students
			(id INTEGER PRIMARY KEY, 
			fname TEXT, 
			lname TEXT, 
			email TEXT, 
			goodstanding INTEGER, 
			current INTEGER)''')
			
			cur.execute('CREATE TABLE IF NOT EXISTS organizations (name TEXT PRIMARY KEY)')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS groups 
			(id INTEGER PRIMARY KEY, 
			organization TEXT REFERENCES organizations(name) ON DELETE CASCADE ON UPDATE CASCAD, 
			semester TEXT NOT NULL REFERENCES semesters(name) ON DELETE CASCADE ON UPDATE CASCADE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS group_memberships 
			(student INTEGER NOT NULL REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE,
			group INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE ON UPDATE CASCADE,
			credit INTEGER NOT NULL
			PRIMARY KEY(student, group))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS absences
			(student INTEGER REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			type TEXT, 
			eventdt TEXT REFERENCES events(dt) ON DELETE CASCADE ON UPDATE CASCADE, 
			excuseid TEXT REFERENCES excuses(id) ON DELETE CASCADE ON UPDATE CASCADE 
			CONSTRAINT pk_absence PRIMARY KEY (eventdt, student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS excuses
			(id INTGER PRIMARY KEY,
			dt TEXT, 
			eventdt TEXT REFERENCES events(dt) ON DELETE CASCADE ON UPDATE CASCADE,
			reason TEXT, 
			student INTEGER REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS signins
			(dt TEXT, 
			eventdt TEXT REFERENCES events(dt) ON DELETE CASCADE ON UPDATE CASCADE,
			student INTEGER REFERENCES students(id) ON DELETE CASCADE ON UPDATE CASCADE
			CONSTRAINT pk_signin PRIMARY KEY (dt, student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS events
			(eventname TEXT, 
			dt TEXT PRIMARY KEY, 
			eventtype TEXT,
			group INTEGER REFERENCES groups(id) ON DELETE CASCADE ON UPDATE CASCADE, 
			semester TEXT REFERENCES semesters(name) ON DELETE CASCADE ON UPDATE CASCADE)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS terms
			(name TEXT PRIMARY KEY,
			startdate TEXT,
			enddate TEXT)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS semesters
			(name TEXT PRIMARY KEY,
			termone TEXT REFERENCES terms(name) ON DELETE RESTRICT ON UPDATE CASCADE,
			termtwo TEXT REFERENCES terms(name) ON DELETE RESTRICT ON UPDATE CASCADE)''')
			
			# Days where WPI closed (holidays, snow days, etc)
			cur.execute('CREATE TABLE IF NOT EXISTS daysoff date TEXT')
			
		finally:
			cur.close()
			con.close()
	
	def read_attendance(self, infile, db=gcdb, connection=None):
		''' Parse the attendance record spreadsheet and write to the database. ''' 
		try:
			signins = []
			with open(infile, 'rb') as f:
				reader = csv.reader(f, delimiter=',')
				for row in reader:
					# row[0] is the mystery blank column
					# row[1] is the date MM/D/YYYY
					# row[2] is the 24-hour time HH:MM 
					# row[3] is the RFID number
					date = row[1].split('/')
					time = row[2].split(':')
					dt = datetime.datetime(int(date[2]), int(date[0]), int(date[1]), int(time[0]), int(time[1]))
					# The record variable formatting matches the Sigin.__init__ arguments list
					record = (isoformat(dt), 'NULL', int(row[3]))
					signins.append(record)
			
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				cur = connection.cursor()
			
			for t in signins:
				# Check that student ID is in DB, if not, create a blank entry
				if Student.select_by_id(t[3]) == None:
					new_student = Student(t[3], 'NULL', 'NULL', 'NULL')
					print 'Adding unknown member to database, RFID# = ', t[3]
					new_student.insert(db, connection)
			cur.executemany('INSERT OR ABORT INTO signins VALUES (?,?,?)', signins)
			
		finally:
			cur.close()
			if connection is None:
				con.close()

gcdb = AttendanceDB()

class RosterException(Exception):
	''' Exception raised when something goes wrong parsing a roster spreadsheet. '''
	def __init__(self, source, text):
		self.source = source
		self.text = text
	def __str__(self):
		return repr(self.text)
		

class Term:
	''' Corresponds to one 7-week term on WPI's academic calendar. '''
	
	@staticmethod
	def select_by_name(name='*', db=gcdb, connection=None):
		''' Return the Term of given name. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (name,)
			cur.execute('SELECT * FROM terms WHERE name=?', params)
			row = cur.fetchone()
			if row != None:
				term = Term(row[0], convert_date(row[1]), convert_date(row[2]))
			else:
				term = None
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return term
	
	@staticmethod
	def select_by_date(start_date='*', end_date='*', db=gcdb, connection=None):
		''' Return the list of Terms in a given datetime range. 
		Any Term whose startdate or enddate column falls within the
		given range will be returned. '''
		terms = []
		
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_date, end_date, start_date, end_date,)
			cur.execute('''SELECT * FROM terms WHERE startdate BETWEEN ? AND ? UNION
			SELECT * FROM terms WHERE enddate BETWEEN ? AND ?''', params)
			for row in cur.fetchall():
				term = Term(row[0], convert_date(row[1]), convert_date(row[2]))
				terms.append(term)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return terms
	
	@staticmethod
	def select_by_all(name='*', start_date='*', end_date='*', db=gcdb, connection=None):
		''' Return a list of Terms using any combination of filters. '''
		terms = []
		
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
			
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_date, end_date, start_date, end_date, name,)
			cur.execute('''SELECT * FROM terms WHERE startdate BETWEEN ? AND ? UNION
			SELECT * FROM terms WHERE enddate BETWEEN ? AND ? INTERSECT
			SELECT * FROM terms WHERE name=?''', params)
			for row in cur.fetchall():
				term = Term(row[0], convert_date(row[1]), convert_date(row[2]))
				terms.append(term)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return terms
	
	def __init__(self, name, start_date, end_date, days_off=[]):
		self.name = name				# Something like "A09", "D12", etc. Primary key.
		self.start_date = start_date	# A date object
		self.end_date = end_date		# A date object
		self.days_off = []	# A list of dates that class is cancelled (holidays, snow days, etc)
		for d in days_off:
			self.days_off.append(d)	
	
	def fetch_days_off(self, db=gcdb, connection=None):
		''' Fetch all daysoff table entries for this Term from the database. 
		Returns a list of date objects. '''
		result = []
		try:
			if type(self.start_date == date):
				start = isoformat(self.start_date)
			elif type(self.start_date == str):
				start = self.start_date
			else:
				raise TypeError
				
			if type(self.end_date == date):
				end = isoformat(self.end_date)
			elif type(self.end_date == str):
				end = self.end_date
			else:
				raise TypeError
		
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start, end,)
			cur.execute('SELECT * FROM daysoff WHERE date BETWEEN ? AND ?', params)
			for row in cur.fetchall():
				result.append(convert_date(row[0]))
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return result
			
	def update(self, db=gcdb, connection=None):
		''' Update an existing Term record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, isoformat(self.start_date), isoformat(self.end_date), self.name,)
			cur.execute('''UPDATE terms 
			SET name=?, startdate=?, enddate=? 
			WHERE name=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Term to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, isoformat(self.start_date), isoformat(self.end_date), )
			cur.execute('INSERT INTO terms VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Term from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name,)
			cur.execute('DELETE FROM terms WHERE name=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()

class Semester:
	''' Corresponds to one 2-term semester on WPI's academic calendar. '''
	
	@staticmethod
	def select_by_name(name='*', db=gcdb, connection=None):
		''' Return the Semester of given name. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (name,)
			cur.execute('SELECT * FROM semester WHERE name=?', params)
			row = cur.fetchone()
			if row != None:
				t1 = Term.select_by_name(row[1], db, con)
				t2 = Term.select_by_name(row[2], db, con)
				semester = Term(row[0], t1, t2)
			else:
				semester = None
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return semester
	
	@staticmethod
	def select_by_date(start_date='*', end_date='*', db=gcdb, connection=None):
		''' Return the list of Semesters in a given datetime range. 
		Any Semester whose startdate or enddate falls within the
		given range will be returned. '''
		terms = []
		semesters = []
		tnames = set()
		snames = set()
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
			
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, )
			cur.execute('''
			SELECT * from semesters WHERE termone IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?) 
			UNION SELECT * from semesters WHERE termtwo IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?)
			 ''', params)
			for row in cur.fetchall():
				t1 = Term.select_by_name(row[1], db, con)
				t2 = Term.select_by_name(row[2], db, con)
				semester = Semester(row[0], t1, t2)
				semesters.append(semester)
							
		finally:
			cur.close()
			if connection is None:
				con.close()
			return semesters
	
	@staticmethod
	def select_by_all(name='*', start_date='*', end_date='*', db=gcdb, connection=None):
		''' Return a list of Semesters using any combination of filters. '''
		semesters = []
		
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
			
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, name,)
			cur.execute('''
			SELECT * from semesters WHERE termone IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?) 
			UNION SELECT * from semesters WHERE termtwo IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?) INTERSECT 
			SELECT * FROM terms WHERE name=?''', params)
			for row in cur.fetchall():
				t1 = Term.select_by_name(row[1], db, con)
				t2 = Term.select_by_name(row[2], db, con)
				semester = Semester(row[0], t1, t2)
				semesters.append(semester)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return semesters
	
	def __init__(self, name, term_one, term_two):
		self.name = name			# Something like "Fall 2011". Primary key.
		self.term_one = term_one	# A or C term
		self.term_two = term_two	# B or D term
		
	def fetch_days_off(self, db=gcdb, connection=None):
		''' Fetch all daysoff table entries for this Semester from the database. 
		Returns a list of date objects. '''
		result = []
		try:
			if type(self.term_one.start_date == date):
				start = isoformat(self.term_one.start_date)
			elif type(self.term_one.start_date == str):
				start = self.term_one.start_date
			else:
				raise TypeError
				
			if type(self.term_two.end_date == date):
				end = isoformat(self.term_two.end_date)
			elif type(self.term_two.end_date == str):
				end = self.term_two.end_date
			else:
				raise TypeError
		
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start, end,)
			cur.execute('SELECT * FROM daysoff WHERE date BETWEEN ? AND ?', params)
			for row in cur.fetchall():
				result.append(convert_date(row[0]))
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return result
			
	def update(self, db=gcdb, connection=None):
		''' Update an existing Semester record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, self.term_one.name, self.term_two.name, self.name,)
			cur.execute('''UPDATE semesters 
			SET name=?, termone=?, termtwo=? 
			WHERE name=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Semester to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, self.term_one.name, self.term_two.name,)
			cur.execute('INSERT INTO semesters VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Semester from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name,)
			cur.execute('DELETE FROM semesters WHERE name=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()

class Student:
	''' A Student who has signed into the attendance system. 
	The student's RFID ID number is the primary key column.	'''
	
	@staticmethod
	def select_by_id(id, db=gcdb, connection=None):
		''' Return the Student of given ID. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (id,)
			cur.execute('SELECT * FROM students WHERE id=?', params)
			row = cur.fetchone()
			if row != None:
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
			else:
				student = None
			
		finally:
			cur.close()
			if connection is None:
				con.close()
			return student
	
	@staticmethod
	def select_by_name(fname='*', lname='*', db=gcdb, connection=None):
		''' Return the Student(s) of given name. '''
		students = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (fname, lname,)
			cur.execute('SELECT * FROM students WHERE fname=? AND lname=?', params)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return students
	
	@staticmethod
	def select_by_email(email, db=gcdb, connection=None):
		''' Return the Student(s) with given email address. '''
		students = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (email,)
			cur.execute('SELECT * FROM students WHERE email=?', params)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return students
	
	@staticmethod
	def select_by_standing(good_standing, db=gcdb, connection=None):
		''' Return the list of Students of given standing. '''
		students = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (int(good_standing),)
			cur.execute('SELECT * FROM students WHERE goodstanding=?', params)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return students
	
	@staticmethod
	def select_by_group(group_id, in_group=True, db=gcdb, connection=None):
		''' Return the list of Students in some group (or not). '''
		students = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (group_id,)			
			if in_group == True:
				cur.execute("""SELECT * FROM students WHERE id IN
				(SELECT DISTINCT student FROM group_memberships WHERE id=?)""", params)
			else:
				cur.execute("""SELECT * FROM students WHERE id NOT IN
				(SELECT DISTINCT student FROM group_memberships WHERE id=?)""", params)

			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return students
	
	@staticmethod
	def select_by_current(current=True, db=gcdb, connection=None):
		''' Return the list of current Students on the roster (or not). '''
		students = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (int(current),)
			cur.execute('SELECT * FROM students WHERE current=?', params)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return students
		
	@staticmethod
	def select_by_all(id='*', fname='*', lname='*', email='*', standing='*', current='*', db=gcdb, connection=None):
		''' Return a list of Students using any combination of filters. '''
		if standing != '*':
			standing = int(standing)
			
		if current != '*':
			current = int(current)
			
		students = []
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (id, fname, lname, email, standing, current,)
			cur.execute('''SELECT * FROM students WHERE id=? INTERSECT 
			SELECT * FROM students WHERE fname=? INTERSECT 
			SELECT * FROM students WHERE lname=? INTERSECT 
			SELECT * FROM students WHERE email=? INTERSECT 
			SELECT * FROM students WHERE goodstanding=? INTERSECT 
			SELECT * FROM students WHERE current=?''', params)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return students
	
	@staticmethod
	def merge(old, new, db=gcdb, connection=None):
		''' Merge the records of one student into another, deleting the first.
		This should be used when a student replaces their ID card, as the new
		ID card will have a different RFID number. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (new.rfid, old.rfid, )
			cur.execute('UPDATE excuses SET student=? WHERE student=?', params)
			cur.execute('UPDATE signins SET student=? WHERE student=?', params)
			cur.execute('UPDATE absences SET student=? WHERE student=?', params)
			
			old.delete()
		
		finally:
			cur.close()
			if connection is None:
				con.close()
			new.fetch_signins(db, connection)
			new.fetch_excuses(db, connection)
	
	def __init__(self, r, fn, ln, email, standing=True, current=True):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.good_standing = standing
		self.current = current # Set false when no longer in active roster
		self.signins = []
		self.excuses = []
		self.absences = []
		self.groups = []
		
	def fetch_signins(self, db=gcdb, connection=None):
		''' Fetch all Signins by this Student from the database. '''
		self.signins = Signin.select_by_student(self.rfid, db, connection)
	
	def fetch_excuses(self, db=gcdb, connection=None):
		''' Fetch all Excuses by this Student from the database. '''
		self.excuses = Excuse.select_by_student(self.rfid, db, connection)
	
	def fetch_absences(self, db=gcdb, connection=None):
		''' Fetch all Absences by this Student from the database. '''
		self.absences = Absence.select_by_student(self.rfid, db, connection)
	
	def fetch_groups(self, db=gcdb, connection=None):
		''' Fetch all Groups this Student is a member of from the database. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name,)
			cur.execute("""SELECT * FROM groups WHERE name IN
					(SELECT DISTINCT group FROM group_memberships WHERE student=?)""", params)
			rows = cur.fetchall()
			for row in rows:
				group = Group(row[0], row[1], row[2])
				group.fetch_members(db, con)
				self.groups.append(group)
			
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def join_group(self, group, credit, db=gcdb, connection=None):
		''' Add the Student to a Group. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, group.id, int(credit))
			cur.execute('INSERT INTO group_memberships VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			self.groups.append(group)
	
	def leave_group(self, group, db=gcdb, connection=None):
		''' Remove the Student from a Group. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, group.id,)
			cur.execute('DELETE FROM group_memberships WHERE student=? AND group=?', params)
			del self.groups[self.groups.index(group)]
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def update(self, db=gcdb, connection=None):
		''' Update an existing Student record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.fname, self.lname, self.email, self.good_standing, self.current, self.rfid,)
			cur.execute('''UPDATE students 
			SET fname=?, lname=?, email=?, goodstanding=?, current=? 
			WHERE id=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Student to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.rfid, self.fname, self.lname, self.email, self.good_standing, self.current,)
			cur.execute('INSERT INTO students VALUES (?,?,?,?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Student from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.rfid,)
			cur.execute('DELETE FROM students WHERE id=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()

class Organization:
	'''An organization that uses the RFID reader for attendance. '''
	
	def __init__(self, name):
		self.name = name

class Group:
	''' A group of students. 
	This is usually an ensemble's roster for a semester. 
	Each Group has a parent Organization.'''
	
	@staticmethod
	def select_by_id(gid, db=gcdb, connection=None):
		''' Return the Group of given ID. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (gid,)
			cur.execute('SELECT * FROM groups WHERE id=?', params)
			row = cur.fetchone()
			if row != None:
				if row[1] == 'NULL':
					organization = None
				else:
					organization = Organization(row[1])
				if row[2] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[2], db, con)
				group = Group(row[0], organization, semester)
			else:
				group = None
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return group
	
	@staticmethod
	def select_by_organization(organization='*', db=gcdb, connection=None):
		''' Return the Group(s) of given parent Organization name. '''
		groups = []
		try:
			if hasattr(organization, name):	# Probably an Organization object
				org = organization.name
			elif isinstance(organization, basestring):
				org = organization
			else:
				raise TypeError
				
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (org,)
			cur.execute('SELECT * FROM groups WHERE organization=?', params)
			for row in cur.fetchall():
				if row[1] == 'NULL':
					organization = None
				else:
					organization = Organization(row[1])
				if row[2] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[2], db, con)
				group = Group(row[0], organization, semester)
				groups.append(group)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return groups
		
	@staticmethod
	def select_by_semester(semester='*', db=gcdb, connection=None):
		''' Return the Group(s) of given Semester. '''
		groups = []
		try:
			if hasattr(semester, term_one):	# Probably a Semester object
				sem = semester.name
			elif isinstance(semester, basestring):
				sem = semester
			else:
				raise TypeError
			
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (sem,)
			cur.execute('SELECT * FROM groups WHERE semester=?', params)
			for row in cur.fetchall():
				if row[1] == 'NULL':
					organization = None
				else:
					organization = Organization(row[1])
				if row[2] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[2], db, con)
				group = Group(row[0], organization, semester)
				groups.append(group)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return groups
	
	def __init__(self, id, organization, semester, students=[]):
		self.id = id
		self.organization = organization
		self.semester = semester
		self.members = students
		
	def fetch_members(self, db=gcdb, connection=None):
		''' Fetch all Students in this group from the database. '''
		self.members = Student.select_by_group(self.id, True)
	
	def add_member(self, student, credit, db=gcdb, connection=None):
		''' Add a new member to the group. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (student.name, self.id, int(credit),)
			cur.execute('INSERT OR ABORT INTO group_memberships VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			self.members.append(student)
	
	def remove_member(self, student, db=gcdb, connection=None):
		''' Remove a member from the group. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (student.name, self.id,)
			cur.execute('DELETE FROM group_memberships WHERE student=? AND group=?', params)
			del self.members[self.members.index(student)]
				
		finally:
			cur.close()
			if connection is None:
				con.close()
		
	def update(self, db=gcdb, connection=None):
		''' Update an existing Group record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.id, self.name, self.semester.name, self.id,)
			cur.execute('UPDATE groups SET id=?, name=?, semester=? WHERE id=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Group to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.id, self.name, self.semester.name,)
			cur.execute('INSERT INTO groups VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Group from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.id,)
			cur.execute('DELETE FROM groups WHERE id=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			
	def read_gc_roster(self, infile, db=gcdb, connection=None):
		''' Parse the group's roster into the database using the Glee Club roster format. '''
		book = Workbook(infile)
		sheet = book['Sheet1']
		rfid_col = 0
		fname_col = 1
		lname_col = 2
		email_col = 3
		shm_col = 4
		cred_col = 5
		officer_col = 6
		for row, cells in sheet.rows().iteritems():	# row is the row number
			if row == 1: # skip header
				continue
			student = Student(cells[rfid_col].value, cells[fname_col].value, cells[lname_col].value, cells[email_col].value)
			if Student.select_by_id(student.id, db, connection) is None:	# Not in DB
				student.insert(db, connection)
			else:
				student.update(db, connection)
			if '1' in cells[cred_col].value or 'y' in string.lower(cells[cred_col].value) or 't' in string.lower(cells[cred_col].value):
				credit = True
			elif '0' in cells[cred_col].value or 'n' in string.lower(cells[cred_col].value) or 'f' in string.lower(cells[cred_col].value):
				credit = False
			else:
				raise RosterException(self.read_gc_roster.__name__, "Failure parsing contents of credit column in roster row " + row)
			self.add_member(student, credit, db, connection)

class Absence:
	''' An instance of a Student not singing into an Event.
	May or may not have an Excuse attached to it. '''
	TYPE_PENDING = "Pending"
	TYPE_EXCUSED = "Excused"
	TYPE_UNEXCUSED = "Unexcused"
	
	@staticmethod
	def select_by_student(student_id='*', db=gcdb, connection=None):
		''' Return the list of Absences by a Student. '''
		absences = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (student_id,)
			cur.execute('SELECT * FROM absences WHERE student=?', params)
			for row in cur.fetchall():
				if row[0] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[0], db, connection)
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[3] == 'NULL':
					excuse = None
				else:
					excuse = Excuse.select_by_id(row[3], db, con)
				absence = Absence(student, row[1], event, excuse)
				absences.append(absence)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return absences
	
	@staticmethod
	def select_by_type(absence_type='*', db=gcdb, connection=None):
		''' Return the list of Absences of a given ABSENCE.TYPE_. '''
		absences = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (absence_type,)
			cur.execute('SELECT * FROM absences WHERE type=?', params)
			for row in cur.fetchall():
				if row[0] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[0], db, connection)
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[3] == 'NULL':
					excuse = None
				else:
					excuse = Excuse.select_by_id(row[3], db, con)
				absence = Absence(student, row[1], event, excuse)
				absences.append(absence)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return absences
		
	@staticmethod
	def select_by_event_dt(event_dt='*', db=gcdb, connection=None):
		''' Return the list of Absences of a given datetime. '''
		absences = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
			
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (event_dt,)
			cur.execute('SELECT * FROM absences WHERE eventdt=?', params)
			for row in cur.fetchall():
				if row[0] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[0], db, connection)
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[3] == 'NULL':
					excuse = None
				else:
					excuse = Excuse.select_by_id(row[3], db, con)
				absence = Absence(student, row[1], event, excuse)
				absences.append(absence)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return absences
	
	@staticmethod
	def select_by_excuse(excuse_id='*', db=gcdb, connection=None):
		''' Return the list of Absences of a given excuse ID.
		Should only return one, but returning a list in case of
		data integrity issues related to Excuse-Event mis-assignment. '''
		absences = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (excuse_id,)
			cur.execute('SELECT * FROM absences WHERE excuseid=?', params)
			for row in cur.fetchall():
				if row[0] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[0], db, connection)
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[3] == 'NULL':
					excuse = None
				else:
					excuse = Excuse.select_by_id(row[3], db, con)
				absence = Absence(student, row[1], event, excuse)
				absences.append(absence)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return absences
	
	@staticmethod
	def select_by_all(student_id='*', absence_type='*', event_dt='*', excuse_id='*', db=gcdb, connection=None):
		''' Return the list of Absences using any combination of filters. '''
		absences = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (student_id, absence_type, event_dt, excuse_id,)
			cur.execute('''SELECT * FROM absences WHERE student=? INTERSECT
			SELECT * FROM absences WHERE type=? INTERSECT
			SELECT * FROM absences WHERE eventdt=? INTERSECT
			SELECT * FROM absences WHERE excuseid=?''', params)
			for row in cur.fetchall():
				if row[0] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[0], db, connection)
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[3] == 'NULL':
					excuse = None
				else:
					excuse = Excuse.select_by_id(row[3], db, con)
				absence = Absence(student, row[1], event, excuse)
				absences.append(absence)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return absences
	
	def __init__(self, student, t, event, excuse=None):
		self.student = student	# A Student object
		self.type = t				# An Absence.TYPE_ string constant
		self.event = event		# An Event object
		self.excuse = excuse	# An Excuse object
	
	def update(self, db=gcdb, connection=None):
		''' Update an existing Absence record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.student.rfid, self.type, isoformat(self.event.event_dt), self.excuse.id, self.student.rfid, isoformat(self.event.event_dt),)
			cur.execute('''UPDATE absences 
			SET student=?, type=?, eventdt=?, excuseid=? 
			WHERE student=? AND eventdt=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Absence to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.student, self.type, isoformat(self.event.event_dt), self.excuse_id,)
			cur.execute('INSERT INTO absences VALUES (?,?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Absence from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.student.rfid, isoformat(self.event.event_dt),)
			cur.execute('DELETE FROM absences WHERE student=? AND eventdt=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
class Excuse:
	''' A Student's excuse for missing an Event sent to gc-excuse. 
	The datetime and student ID are the primary key colums.'''
	
	# Cutoffs for when students can email gc-excuse (relative to event start time)
	EXCUSES_OPENS = datetime.timedelta(-1, 0, 0, 0, 0, -18, 0)	# 1 day, 18 hours before
	EXCUSES_CLOSES = datetime.timedelta(0, 0, 0, 0, 0, 6, 0)	# 6 hours after
	
	@staticmethod
	def select_by_id(excuse_id, db=gcdb, connection=None):
		''' Return the Excuse of given unique ID. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (excuse_id,)
			cur.execute('SELECT * FROM excuses WHERE id=?', params)
			row = cur.fetchone()
			if row != None:
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[4] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[4], db, connection)
				excuse = Excuse(int(row[0]), convert_timestamp(row[1]), event, row[3], student)
			else:
				excuse = None
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return excuse
	
	@staticmethod
	def select_by_student(student_id='*', db=gcdb, connection=None):
		''' Return the list of Excuses by a Student. '''
		excuses = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (student_id,)
			cur.execute('SELECT * FROM excuses WHERE student=?', params)
			for row in cur.fetchall():
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[4] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[4], db, connection)
				excuse = Excuse(int(row[0]), convert_timestamp(row[1]), event, row[3], student)
				excuses.append(excuse)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return excuses
	
	@staticmethod
	def select_by_datetime_range(start_dt='*', end_dt='*', db=gcdb, connection=None):
		''' Return the list of Excuses in a given datetime range. '''
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		excuses = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_dt, end_dt,)
			cur.execute('SELECT * FROM excuses WHERE dt BETWEEN ? AND ?', params)
			for row in cur.fetchall():
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[4] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[4], db, connection)
				excuse = Excuse(int(row[0]), convert_timestamp(row[1]), event, row[3], student)
				excuses.append(excuse)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return excuses
		
	@staticmethod
	def select_by_event_datetime(event_dt='*', db=gcdb, connection=None):
		''' Return the list of Excuses associated with a given Event. '''
		excuses = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (event_dt,)
			cur.execute('SELECT * FROM excuses WHERE eventdt=?', params)
			for row in cur.fetchall():
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[4] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[4], db, connection)
				excuse = Excuse(int(row[0]), convert_timestamp(row[1]), event, row[3], student)
				excuses.append(excuse)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return excuses
		
	@staticmethod
	def select_by_all(excuse_id='*', student_id='*', start_dt='*', end_dt='*', event_dt='*', db=gcdb, connection=None):
		''' Return a list of Excuses using any combination of filters. '''
		excuses = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (excuse_id, student_id, start_dt, end_dt, event_dt,)
			cur.execute('''SELECT * FROM excuses WHERE id=? INTERSECT 
			SELECT * FROM excuses WHERE student=? INTERSECT 
			SELECT * FROM excuses WHERE dt BETWEEN ? AND ? INTERSECT 
			SELECT * FROM excuses WHERE eventdt=?''', params)
			for row in cur.fetchall():
				if row[2] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[2], db, con)[0]
				if row[4] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[4], db, connection)
				excuse = Excuse(int(row[0]), convert_timestamp(row[1]), event, row[3], student)
				excuses.append(excuse)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return excuses
	 
	def __init__(self, id, dt, event, reason, s):
		self.id = id				# Unique primary key
		self.excuse_dt = dt			# a datetime object
		self.event = event			# an Event object
		self.reason = reason		# Student's message to gc-excuse
		self.student = s			# a Student object
	
	def update(self, db=gcdb, connection=None):
		''' Update an existing Excuse record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.excuse_dt), isoformat(self.event.event_dt), self.reason, self.student.rfid, self.id,)
			cur.execute('''UPDATE excuses 
			SET dt=?, eventdt=?, reason=?, student=? 
			WHERE id=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Excuse to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.excuse_dt), isoformat(self.event.event_dt), self.reason, self.student.rfid,)
			# INSERTing 'NULL' for the integer primary key column autogenerates an id
			cur.execute('INSERT INTO excuses VALUES (NULL,?,?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Excuse from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.id,)
			cur.execute('DELETE FROM excuses WHERE id=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
class Signin:
	''' Corresponds to a row in the RFID output file. 
	The datetime and student ID are the primary key colums.'''
	
	@staticmethod
	def select_by_student(id, db=gcdb, connection=None):
		''' Return the list of Signins by a Student. '''
		signins = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (id,)
			cur.execute('SELECT * FROM signins WHERE student=?', params)
			for row in cur.fetchall():
				if row[1] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[1], db, con)[0]
				if row[2] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[2], db, con)
				signin = Signin(convert_timestamp(row[0]), event, student)
				signins.append(signin)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return signins
	
	@staticmethod
	def select_by_datetime(start_dt, end_dt, db=gcdb, connection=None):
		''' Return the list of Signins in a given datetime range. '''
		signins = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_dt, end_dt,)
			cur.execute('SELECT * FROM signins WHERE dt BETWEEN ? AND ?', params)
			for row in cur.fetchall():
				if row[1] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[1], db, con)[0]
				if row[2] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[2], db, con)
				signin = Signin(convert_timestamp(row[0]), event, student)
				signins.append(signin)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return signins
	
	@staticmethod
	def select_by_event_datetime(event_dt, db=gcdb, connection=None):
		''' Return the list of Signins associated with a given Event. '''
		signins = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(event_dt),)
			cur.execute('SELECT * FROM signins WHERE eventdt=?', params)
			for row in cur.fetchall():
				if row[1] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[1], db, con)[0]
				if row[2] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[2], db, con)
				signin = Signin(convert_timestamp(row[0]), event, student)
				signins.append(signin)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return signins
	
	@staticmethod
	def select_by_all(id='*', start_dt='*', end_dt='*', event_dt='*', db=gcdb, connection=None):
		''' Return a list of Signins using any combination of filters. '''
		signins = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
			
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (id, start_dt, end_dt, event_dt,)
			cur.execute('''SELECT * FROM signins WHERE student=? INTERSECT
			 SELECT * FROM signins WHERE dt BETWEEN ? AND ? INTERSECT 
			 SELECT * FROM signins WHERE eventdt=?''', params)
			for row in cur.fetchall():
				if row[1] == 'NULL':
					event = None
				else:
					event = Event.select_by_datetime(row[1], db, con)[0]
				if row[2] == 'NULL':
					student = None
				else:
					student = Student.select_by_id(row[2], db, con)
				signin = Signin(convert_timestamp(row[0]), event, student)
				signins.append(signin)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return signins
	
	def __init__(self, dt, event, student):
		self.signin_dt = dt			# a datetime object
		self.event = event			# an Event object
		self.student = student		# a Student object
	
	def guess_event(self, db=gcdb, connection=None):
		''' Search the database for events that this Signin might correspond to.
		Likely events are defined as starting within 2 hours of self.signin_dt
		(to allow for showing up and/or signing in late) and are held by a group
		that self.student is a member of.
		Returns a list of Event objects without setting self.event directly. '''
		time_window = datetime.timedelta(0, 0, 0, 0, 0, 2, 0)	# 2 hours
		try:
			events = []
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.signin_dt - time_window), isoformat(self.signin_dt + time_window), self.student.rfid,) 
			cur.execute('''SELECT * FROM events WHERE dt BETWEEN ? AND ? INTERSECT 
			SELECT * FROM events WHERE group IN 
			(SELECT group FROM group_memberships WHERE student=?)''')
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
		
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	def update(self, db=gcdb, connection=None):
		''' Update an existing Signin record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.signin_dt), isoformat(self.event.event_dt), self.student.rfid, isoformat(self.event.event_dt), self.student.rfid,)
			cur.execute('''UPDATE signins 
			SET dt=?, eventdt=?, student=? 
			WHERE dt=? AND student=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Signin to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.signin_dt), isoformat(self.event.event_dt), self.student.rfid,)
			cur.execute('INSERT OR ABORT INTO signins VALUES (?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Signin from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.signin_dt), self.student.rfid,)
			cur.execute('DELETE FROM signins WHERE dt=? AND student=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()

class Event:
	''' An event where attendance is taken. 
	The datetime is the primary key column.'''
	TYPE_REHEARSAL = 'Rehearsal'
	TYPE_MAKEUP = 'Makeup Rehearsal'
	TYPE_DRESS = 'Dress Rehearsal'	# Mandatory for a concert
	TYPE_CONCERT = 'Concert'
	
	# Cutoffs for when students can sign in (relative to event start time)
	ATTENDANCE_OPENS = datetime.timedelta(0, 0, 0, 0, -30, 0, 0)	# 30 minutes before
	ATTENDANCE_CLOSES = datetime.timedelta(0, 0, 0, 0, 30, 1, 0)	# 90 minutes after
	
	@staticmethod
	def select_by_name(name, db=gcdb, connection=None):
		''' Return the list of Events of a given name. '''
		events = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (name,)
			cur.execute('SELECT * FROM events WHERE eventname=?', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	@staticmethod
	def select_by_datetime(event_dt, db=gcdb, connection=None):
		''' Return the list of Events of a specific datetime. '''
		events = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (event_dt,)
			cur.execute('SELECT * FROM events WHERE dt=?', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	@staticmethod
	def select_by_datetime_range(start_dt, end_dt, db=gcdb, connection=None):
		''' Return the list of Events in a given datetime range. '''
		events = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (start_dt, end_dt,)
			cur.execute('SELECT * FROM events WHERE dt BETWEEN ? AND ?', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	@staticmethod
	def select_by_type(type, db=gcdb, connection=None):
		''' Return the list of Events of a given type. '''
		events = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (type,)
			cur.execute('SELECT * FROM events WHERE eventtype=?', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	@staticmethod
	def select_by_group(group, db=gcdb, connection=None):
		''' Return the list of Events of a given group. '''
		events = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (group,)
			cur.execute('SELECT * FROM events WHERE group=?', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	@staticmethod
	def select_by_semester(semester, db=gcdb, connection=None):
		''' Return the list of Events in a given Semester. '''
		events = []
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (semester,)
			cur.execute('SELECT * FROM events WHERE semester=?', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					semester = None
				else:
					semester = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	@staticmethod
	def select_by_all(name, start_dt, end_dt, type, group, semester, db=gcdb, connection=None):
		''' Return a list of Events using any combination of filters. '''
		events = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (name, start_dt, end_dt, type, group, semester,)
			cur.execute('''SELECT * FROM events WHERE eventname=? INTERSECT 
			SELECT * FROM events WHERE dt BETWEEN ? AND ? INTERSECT 
			SELECT * FROM events WHERE eventtype=? INTERSECT 
			SELECT * FROM events WHERE group=? INTERSECT 
			SELECT * FROM events WHERE semester=?''', params)
			for row in cur.fetchall():
				if row[3] == 'NULL':
					group = None
				else:
					group = Group.select_by_id(row[3], db, con)
				if row[4] == 'NULL':
					semester = None
				else:
					semester = Semester.select_by_name(row[4], db, con)
				event = Event(row[0], convert_timestamp(row[1]), row[2], group, semester)
				events.append(event)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
			return events
	
	def __init__(self, name, dt, t, group, semester):
		self.event_name = name
		self.event_dt = dt		# a datetime object, primary key
		self.event_type = t		# One of the Event.TYPE_ constants 
		self.group = group		# Roster to check against
		self.semester = semester	# A Semester object
		self.signins = []
		self.excuses = []
		self.absences = []
	
	def fetch_signins(self, db=gcdb, connection=None):
		''' Fetch all Signins for this Event from the database. '''
		self.signins = Signin.select_by_datetime(self.event_dt+Event.ATTENDANCE_OPENS, self.event_dt+Event.ATTENDANCE_CLOSES, db, connection)
	
	def fetch_excuses(self, db=gcdb, connection=None):
		''' Fetch all Excuses for this Event from the database. '''
		self.excuses = Excuse.select_by_datetime(self.event_dt+Excuse.EXCUSES_OPENS, self.event_dt+Excuse.EXCUSES_CLOSES, db, connection)
		
	def fetch_absences(self, db=gcdb, connection=None):
		''' Fetch all Absences for this Event from the database. '''
		self.absences = Absence.select_by_event_dt(self.event_dt, db, connection)
	
	def update(self, db=gcdb, connection=None):
		''' Update an existing Event record in the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, isoformat(self.event_dt), self.event_type, self.group.id, isoformat(self.event_dt),)
			cur.execute('''UPDATE events 
			SET eventname=?, dt=?, type=?, group=? 
			WHERE dt=?''', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def insert(self, db=gcdb, connection=None):
		''' Write the Event to the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (self.name, isoformat(self.event_dt), self.event_type, self.group.id,)
			cur.execute('INSERT INTO events VALUES (?,?,?,?)', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()
	
	def delete(self, db=gcdb, connection=None):
		''' Delete the Event from the DB. '''
		try:
			if connection is None:
				(con, cur) = db.con_cursor()
			else:
				con = connection
				cur = con.cursor()
			
			params = (isoformat(self.event_dt), self.group.id,)
			cur.execute('DELETE FROM events WHERE dt=? AND group=?', params)
				
		finally:
			cur.close()
			if connection is None:
				con.close()

	