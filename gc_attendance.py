import csv
import shutil
import os
import sqlite3
import datetime
import types

active_roster = []
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
			# TODO: Add error handling clauses to the foreign key constraints
			cur.execute('''CREATE TABLE IF NOT EXISTS students
			(id INTEGER PRIMARY KEY, 
			fname TEXT, 
			lname TEXT, 
			email TEXT, 
			goodstanding INTEGER, 
			current INTEGER)''')
			
			cur.execute('CREATE TABLE IF NOT EXISTS groups (name TEXT PRIMARY KEY)')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS group_memberships 
			(student INTEGER NOT NULL REFERENCES students(id),
			group TEXT NOT NULL REFERENCES groups(name),
			PRIMARY KEY(student, group))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS absences
			(student INTEGER REFERENCES students(id), 
			type TEXT, 
			eventdt TEXT REFERENCES events(dt), 
			excuseid TEXT REFERENCES excuses(id) 
			CONSTRAINT pk_absence PRIMARY KEY (eventdt, student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS excuses
			(id INTGER PRIMARY KEY
			dt TEXT, 
			eventdt TEXT REFERENCES events(dt),
			reason TEXT, 
			student INTEGER REFERENCES students(id))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS signins
			(dt TEXT, 
			eventdt TEXT REFERENCES events(dt),
			student INTEGER
			CONSTRAINT pk_signin PRIMARY KEY (dt, student),
			CONSTRAINT fk_signin_student FOREIGN KEY (student))''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS events
			(eventname TEXT, 
			dt TEXT PRIMARY KEY, 
			eventtype TEXT)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS terms
			(name TEXT PRIMARY KEY,
			startdate TEXT,
			enddate TEXT)''')
			
			cur.execute('''CREATE TABLE IF NOT EXISTS semesters
			(name TEXT PRIMARY KEY,
			termone TEXT REFERENCES terms(name),
			termtwo TEXT REFERENCES terms(name))''')
			
			# Days where WPI closed (holidays, snow days, etc)
			cur.execute('CREATE TABLE IF NOT EXISTS daysoff date TEXT')
			
		finally:
			cur.close()
			con.close()
	
	def read_attendance(self, infile):
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
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			for t in signins:
				# Check that student ID is in DB, if not, create a blank entry
				if Student.select_by_id(t[3]) == None:
					new_student = Student(t[3], 'NULL', 'NULL', 'NULL')
					print 'Adding unknown member to database, RFID# = ', t[3]
					new_student.insert()
			cur.executemany('INSERT OR ABORT INTO signins VALUES (?,?,?)', signins)
			
		finally:
			cur.close()
			con.close()

gcdb = AttendanceDB()

class Term:
	''' Corresponds to one 7-week term on WPI's academic calendar. '''
	
	@staticmethod
	def select_by_name(name='*'):
		''' Return the Term of given name. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (name,)
			cur.execute('SELECT * FROM terms WHERE name=?', symbol)
			row = cur.fetchone()
			if row != None:
				term = Term(row[0], row[1], row[2])
			else:
				term = None
				
		finally:
			cur.close()
			con.close()
			return term
	
	@staticmethod
	def select_by_date(start_date='*', end_date='*'):
		''' Return the list of Terms in a given datetime range. 
		Any Term whose startdate or enddate column falls within the
		given range will be returned. '''
		terms = []
		
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_date, end_date, start_date, end_date,)
			cur.execute('''SELECT * FROM terms WHERE startdate BETWEEN ? AND ? UNION
			SELECT * FROM terms WHERE enddate BETWEEN ? AND ?''', symbol)
			for row in cur.fetchall():
				term = Term(row[0], row[1], row[2])
				terms.append(term)
				
		finally:
			cur.close()
			con.close()
			return terms
	
	@staticmethod
	def select_by_all(name='*', start_date='*', end_date='*'):
		''' Return a list of Terms using any combination of filters. '''
		terms = []
		
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
			
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_date, end_date, start_date, end_date, name,)
			cur.execute('''SELECT * FROM terms WHERE startdate BETWEEN ? AND ? UNION
			SELECT * FROM terms WHERE enddate BETWEEN ? AND ? INTERSECT
			SELECT * FROM terms WHERE name=?''', symbol)
			for row in cur.fetchall():
				term = Term(row[0], row[1], row[2])
				terms.append(term)
				
		finally:
			cur.close()
			con.close()
			return terms
	
	def __init__(self, name, start_date, end_date, days_off=[]):
		self.name = name				# Something like "A09", "D12", etc. Primary key.
		self.start_date = start_date	# A date object
		self.end_date = end_date		# A date object
		self.days_off = []	# A list of dates that class is cancelled (holidays, snow days, etc)
		for d in days_off:
			self.days_off.append(d)	
	
	def fetch_days_off(self):
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
		
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start, end,)
			cur.execute('SELECT * FROM daysoff WHERE date BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				result.append(convert_date(row[0]))
				
		finally:
			cur.close()
			con.close()
			return result
			
	def update(self):
		''' Update an existing Term record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name, self.start_date, self.end_date, self.name,)
			cur.execute('''UPDATE terms 
			SET name=?, startdate=?, enddate=? 
			WHERE name=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Term to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name, self.start_date, self.end_date, )
			cur.execute('INSERT INTO terms VALUES (?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Term from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name,)
			cur.execute('DELETE FROM terms WHERE name=?', symbol)
				
		finally:
			cur.close()
			con.close()

class Semester:
	''' Corresponds to one 2-term semester on WPI's academic calendar. '''
	
	@staticmethod
	def select_by_name(name='*'):
		''' Return the Semester of given name. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (name,)
			cur.execute('SELECT * FROM semester WHERE name=?', symbol)
			row = cur.fetchone()
			if row != None:
				t1 = Term.select_by_name(row[1])
				t2 = Term.select_by_name(row[2])
				semester = Term(row[0], t1, t2)
			else:
				semester = None
				
		finally:
			cur.close()
			con.close()
			return semester
	
	@staticmethod
	def select_by_date(start_date='*', end_date='*'):
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
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, )
			cur.execute('''
			SELECT * from semesters WHERE termone IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?) 
			UNION SELECT * from semesters WHERE termtwo IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?)
			 ''', symbol)
			for row in cur.fetchall():
				t1 = Term.select_by_name(row[1])
				t2 = Term.select_by_name(row[2])
				semester = Semester(row[0], t1, t2)
				semesters.append(semester)
							
		finally:
			cur.close()
			con.close()
			return semesters
	
	@staticmethod
	def select_by_all(name='*', start_date='*', end_date='*'):
		''' Return a list of Semesters using any combination of filters. '''
		semesters = []
		
		if type(start_date == date):
			start_date = isoformat(start_date)
		if type(end_date == date):
			end_date = isoformat(end_date)
			
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_date, end_date, start_date, end_date, start_date, end_date, start_date, end_date, name,)
			cur.execute('''
			SELECT * from semesters WHERE termone IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?) 
			UNION SELECT * from semesters WHERE termtwo IN 
			(SELECT name FROM terms WHERE startdate BETWEEN ? AND ? UNION 
			SELECT name FROM terms WHERE enddate BETWEEN ? AND ?) INTERSECT 
			SELECT * FROM terms WHERE name=?''', symbol)
			for row in cur.fetchall():
				t1 = Term.select_by_name(row[1])
				t2 = Term.select_by_name(row[2])
				semester = Semester(row[0], t1, t2)
				semesters.append(semester)
				
		finally:
			cur.close()
			con.close()
			return semesters
	
	def __init__(self, name, term_one, term_two):
		self.name = name			# Something like "Fall 2011". Primary key.
		self.term_one = term_one	# A or C term
		self.term_two = term_two	# B or D term
		
	def fetch_days_off(self):
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
		
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start, end,)
			cur.execute('SELECT * FROM daysoff WHERE date BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				result.append(convert_date(row[0]))
				
		finally:
			cur.close()
			con.close()
			return result
			
	def update(self):
		''' Update an existing Semester record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name, self.term_one.name, self.term_two.name, self.name,)
			cur.execute('''UPDATE semesters 
			SET name=?, termone=?, termtwo=? 
			WHERE name=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Semester to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name, self.term_one.name, self.term_two.name,)
			cur.execute('INSERT INTO semesters VALUES (?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Semester from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name,)
			cur.execute('DELETE FROM semesters WHERE name=?', symbol)
				
		finally:
			cur.close()
			con.close()

class Student:
	''' A Student who has signed into the attendance system. 
	The student's RFID ID number is the primary key column.	'''
	
	@staticmethod
	def select_by_id(id):
		''' Return the Student of given ID. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id,)
			cur.execute('SELECT * FROM students WHERE id=?', symbol)
			row = cur.fetchone()
			if row != None:
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
			else:
				student = None
			
		finally:
			cur.close()
			con.close()
			return student
	
	@staticmethod
	def select_by_name(fname='*', lname='*'):
		''' Return the Student(s) of given name. '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (fname, lname,)
			cur.execute('SELECT * FROM students WHERE fname=? AND lname=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_email(email):
		''' Return the Student(s) with given email address. '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (email,)
			cur.execute('SELECT * FROM students WHERE email=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_standing(good_standing):
		''' Return the list of Students of given standing. '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (int(good_standing),)
			cur.execute('SELECT * FROM students WHERE goodstanding=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_group(group_name, in_group=True):
		''' Return the list of Students in some group (or not). '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			params = (group_name,)			
			if in_group == True:
				cur.execute("""SELECT * FROM students WHERE id IN
				(SELECT DISTINCT student FROM group_memberships WHERE group=?)""", params)
			else:
				cur.execute("""SELECT * FROM students WHERE id NOT IN
				(SELECT DISTINCT student FROM group_memberships WHERE group=?)""", params)

			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def select_by_current(current=True):
		''' Return the list of current Students on the roster (or not). '''
		students = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (int(current),)
			cur.execute('SELECT * FROM students WHERE current=?', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			con.close()
			return students
		
	@staticmethod
	def select_by_all(id='*', fname='*', lname='*', email='*', standing='*', current='*'):
		''' Return a list of Students using any combination of filters. '''
		if shm != '*':
			shm = int(shm)
			
		if standing != '*':
			standing = int(standing)
			
		if credit != '*':
			credit = int(credit)
		
		if current != '*':
			current = int(current)
			
		students = []
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id, fname, lname, email, shm, standing, credit, current,)
			cur.execute('''SELECT * FROM students WHERE id=? INTERSECT 
			SELECT * FROM students WHERE fname=? INTERSECT 
			SELECT * FROM students WHERE lname=? INTERSECT 
			SELECT * FROM students WHERE email=? INTERSECT 
			SELECT * FROM students WHERE goodstanding=? INTERSECT 
			SELECT * FROM students WHERE current=?''', symbol)
			for row in cur.fetchall():
				student = Student(row[0], row[1], row[2], row[3], row[4], row[5])
				students.append(student)
				
		finally:
			cur.close()
			con.close()
			return students
	
	@staticmethod
	def merge(old, new):
		''' Merge the records of one student into another, deleting the first.
		This should be used when a student replaces their ID card, as the new
		ID card will have a different RFID number. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (new.rfid, old.rfid, )
			cur.execute('UPDATE excuses SET student=? WHERE student=?', symbol)
			cur.execute('UPDATE signins SET student=? WHERE student=?', symbol)
			cur.execute('UPDATE absences SET student=? WHERE student=?', symbol)
			
			old.delete()
		
		finally:
			cur.close()
			con.close()
			new.fetch_signins()
			new.fetch_excuses()
	
	def __init__(self, r, fn, ln, email, standing=True, current=True):
		self.rfid = r		# Numeric ID seen by the RFID reader
		self.fname = fn
		self.lname = ln
		self.email = email
		self.shm = shm
		self.good_standing = standing
		self.credit = cred	# Taking GC for class credit
		self.current = current # Set false when no longer in active roster
		self.signins = []
		self.excuses = []
		self.absences = []
		self.groups = []
		
	def __del__(self):
		self.delete()
	
	def fetch_signins(self):
		''' Fetch all Signins by this Student from the database. '''
		self.signins = Signin.select_by_student(self.rfid)
	
	def fetch_excuses(self):
		''' Fetch all Excuses by this Student from the database. '''
		self.excuses = Excuse.select_by_student(self.rfid)
	
	def fetch_absences(self):
		''' Fetch all Absences by this Student from the database. '''
		self.absences = Absence.select_by_student(self.rfid)
	
	def fetch_groups(self):
		''' Fetch all Groups this Student is a member of from the database. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (self.name,)
			cur.execute("""SELECT * FROM groups WHERE name IN
					(SELECT DISTINCT group FROM group_memberships WHERE student=?)""", params)
			rows = cur.fetchall()
			for row in rows:
				group = Group(row[0])
				group.fetch_members()
				self.groups.append(group)
			
		finally:
			cur.close()
			con.close()
	
	def join_group(self, group):
		''' Add the Student to a Group. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (self.name, group.name,)
			cur.execute('INSERT INTO group_memberships VALUES (?,?)', params)
				
		finally:
			cur.close()
			con.close()
			self.groups.append(group)
	
	def leave_group(self, group):
		''' Remove the Student from a Group. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (self.name, group.name,)
			cur.execute('DELETE FROM group_memberships WHERE student=? AND group=?', params)
			del self.groups[self.groups.index(group)]
				
		finally:
			cur.close()
			con.close()
	
	def update(self):
		''' Update an existing Student record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.fname, self.lname, self.email, self.shm, self.good_standing, self.credit, self.current, self.rfid,)
			cur.execute('''UPDATE students 
			SET fname=?, lname=?, email=?, shm=?, goodstanding=?, credit=?, current=? 
			WHERE id=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Student to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.rfid, self.fname, self.lname, self.email, self.shm, self.good_standing, self.credit, self.current,)
			cur.execute('INSERT INTO students VALUES (?,?,?,?,?,?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Student from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.rfid,)
			cur.execute('DELETE FROM students WHERE id=?', symbol)
				
		finally:
			cur.close()
			con.close()

class Group:
	''' A group of students. This is usually an ensemble. 
	However, it can also be the group of students participating in an
	ensemble for WPI course credit. '''
	
	@staticmethod
	def select_by_name(name='*'):
		''' Return the Group(s) of given name. '''
		groups = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (name,)
			cur.execute('SELECT * FROM groups WHERE name=?', params)
			for row in cur.fetchall():
				group = Group(row[0])
				groups.append(group)
				
		finally:
			cur.close()
			con.close()
			return groups
	
	def __init__(self, name, students=[]):
		self.name = name
		self.members = students
		
	def fetch_members(self):
		''' Fetch all Students in this group from the database. '''
		self.members = Student.select_by_group(self.name, True)
	
	def add_member(self, student):
		''' Add a new member to the group. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (student.name, self.name,)
			cur.execute('INSERT INTO group_memberships VALUES (?,?)', params)
				
		finally:
			cur.close()
			con.close()
			self.members.append(student)
	
	def remove_member(self, student):
		''' Remove a member from the group. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (student.name, self.name,)
			cur.execute('DELETE FROM group_memberships WHERE student=? AND group=?', params)
			del self.members[self.members.index(student)]
				
		finally:
			cur.close()
			con.close()
		
	def update(self):
		''' Update an existing Group record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (self.name, self.name,)
			cur.execute('UPDATE groups SET name=? WHERE name=?', params)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Group to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (self.name,)
			cur.execute('INSERT INTO groups VALUES (?)', params)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Group from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			params = (self.name,)
			cur.execute('DELETE FROM groups WHERE name=?', params)
				
		finally:
			cur.close()
			con.close()

class Absence:
	''' An instance of a Student not singing into an Event.
	May or may not have an Excuse attached to it. '''
	TYPE_PENDING = "Pending"
	TYPE_EXCUSED = "Excused"
	TYPE_UNEXCUSED = "Unexcused"
	
	@staticmethod
	def select_by_student(student_id='*'):
		''' Return the list of Absences by a Student. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (student_id,)
			cur.execute('SELECT * FROM absences WHERE student=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		finally:
			cur.close()
			con.close()
			return absences
	
	@staticmethod
	def select_by_type(absence_type='*'):
		''' Return the list of Absences of a given ABSENCE.TYPE_. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (absence_type,)
			cur.execute('SELECT * FROM absences WHERE type=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		finally:
			cur.close()
			con.close()
			return absences
		
	@staticmethod
	def select_by_event_dt(event_dt='*'):
		''' Return the list of Absences of a given datetime. '''
		absences = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
			
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (event_dt,)
			cur.execute('SELECT * FROM absences WHERE eventdt=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		finally:
			cur.close()
			con.close()
			return absences
	
	@staticmethod
	def select_by_excuse(excuse_id='*'):
		''' Return the list of Absences of a given excuse ID.
		Should only return one, but returning a list in case of
		data integrity issues related to Excuse-Event mis-assignment. '''
		absences = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (excuse_id,)
			cur.execute('SELECT * FROM absences WHERE excuseid=?', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		finally:
			cur.close()
			con.close()
			return absences
	
	@staticmethod
	def select_by_all(student_id='*', absence_type='*', event_dt='*', excuse_id='*'):
		''' Return the list of Absences using any combination of filters. '''
		absences = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (student_id, absence_type, event_dt, excuse_id,)
			cur.execute('''SELECT * FROM absences WHERE student=? INTERSECT
			SELECT * FROM absences WHERE type=? INTERSECT
			SELECT * FROM absences WHERE eventdt=? INTERSECT
			SELECT * FROM absences WHERE excuseid=?''', symbol)
			for row in cur.fetchall():
				absence = Absence(row[0], row[1], row[2], row[3])
				absences.append(absence)
				
		finally:
			cur.close()
			con.close()
			return absences
	
	def __init__(self, student_id, t, event_dt, excuse_id=None):
		self.student = student_id
		self.type = t				# An Absence.TYPE_ string constant
		self.event_dt = event_dt	# Get the actual event via dt lookup
		self.excuse_id = excuse_id
	
	def __del__(self):
		self.delete()
		
	def update(self):
		''' Update an existing Absence record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.student, self.type, self.event_dt, self.excuse_id, self.student, self.event_dt,)
			cur.execute('''UPDATE absences 
			SET student=?, type=?, eventdt=?, excuseid=? 
			WHERE student=? AND eventdt=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Absence to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.student, self.type, self.event_dt, self.excuse_id,)
			cur.execute('INSERT INTO absences VALUES (?,?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Absence from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.student, self.event_dt,)
			cur.execute('DELETE FROM absences WHERE student=? AND eventdt=?', symbol)
				
		finally:
			cur.close()
			con.close()
	
class Excuse:
	''' A Student's excuse for missing an Event sent to gc-excuse. 
	The datetime and student ID are the primary key colums.'''
	
	# Cutoffs for when students can email gc-excuse (relative to event start time)
	EXCUSES_OPENS = datetime.timedelta(-1, 0, 0, 0, 0, -18, 0)	# 1 day, 18 hours before
	EXCUSES_CLOSES = datetime.timedelta(0, 0, 0, 0, 0, 6, 0)	# 6 hours after
	
	@staticmethod
	def select_by_id(excuse_id):
		''' Return the Excuse of given unique ID. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (excuse_id,)
			cur.execute('SELECT * FROM excuses WHERE id=?', symbol)
			row = cur.fetchone()
			excuse = Excuse(row[0], row[1], row[2], row[3], row[4])
				
		finally:
			cur.close()
			con.close()
			return excuse
	
	@staticmethod
	def select_by_student(student_id='*'):
		''' Return the list of Excuses by a Student. '''
		excuses = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (student_id,)
			cur.execute('SELECT * FROM excuses WHERE student=?', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3], row[4])
				excuses.append(excuse)
				
		finally:
			cur.close()
			con.close()
			return excuses
	
	@staticmethod
	def select_by_datetime(start_dt='*', end_dt='*'):
		''' Return the list of Excuses in a given datetime range. '''
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		excuses = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_dt, end_dt,)
			cur.execute('SELECT * FROM excuses WHERE dt BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3], row[4])
				excuses.append(excuse)
				
		finally:
			cur.close()
			con.close()
			return excuses
		
	@staticmethod
	def select_by_event_datetime(event_dt='*'):
		''' Return the list of Excuses associated with a given Event. '''
		excuses = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (event_dt,)
			cur.execute('SELECT * FROM excuses WHERE eventdt=?', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3], row[4])
				excuses.append(excuse)
				
		finally:
			cur.close()
			con.close()
			return excuses
		
	@staticmethod
	def select_by_all(excuse_id='*', student_id='*', start_dt='*', end_dt='*', event_dt='*'):
		''' Return a list of Excuses using any combination of filters. '''
		excuses = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (excuse_id, student_id, start_dt, end_dt, event_dt,)
			cur.execute('''SELECT * FROM excuses WHERE id=? INTERSECT 
			SELECT * FROM excuses WHERE student=? INTERSECT 
			SELECT * FROM excuses WHERE dt BETWEEN ? AND ? INTERSECT 
			SELECT * FROM excuses WHERE eventdt=?''', symbol)
			for row in cur.fetchall():
				excuse = Excuse(row[0], row[1], row[2], row[3], row[4])
				excuses.append(excuse)
				
		finally:
			cur.close()
			con.close()
			return excuses
	 
	def __init__(self, id, dt, event_dt, reason, s):
		self.id = id				# Unique primary key
		self.excuse_dt = dt			# a datetime object
		self.event_dt = event_dt	# a datetime object or 'NULL'
		self.reason = reason		# Student's message to gc-excuse
		self.student = s			# RFID number
	
	def __del__(self):
		self.delete()
		
	def update(self):
		''' Update an existing Excuse record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.excuse_dt, self.event_dt, self.reason, self.student, self.id,)
			cur.execute('''UPDATE excuses 
			SET dt=?, eventdt=?, reason=?, student=? 
			WHERE id=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Excuse to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.excuse_dt, self.event_dt, self.reason, self.student,)
			# INSERTing 'NULL' for the integer primary key column autogenerates an id
			cur.execute('INSERT INTO excuses VALUES (NULL,?,?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Excuse from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.id,)
			cur.execute('DELETE FROM excuses WHERE id=?', symbol)
				
		finally:
			cur.close()
			con.close()
	
class Signin:
	''' Corresponds to a row in the RFID output file. 
	The datetime and student ID are the primary key colums.'''
	
	@staticmethod
	def select_by_student(id):
		''' Return the list of Signins by a Student. '''
		signins = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id,)
			cur.execute('SELECT * FROM signins WHERE student=?', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1], row[2])
				signins.append(signin)
				
		finally:
			cur.close()
			con.close()
			return signins
	
	@staticmethod
	def select_by_datetime(start_dt, end_dt):
		''' Return the list of Signins in a given datetime range. '''
		signins = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_dt, end_dt,)
			cur.execute('SELECT * FROM signins WHERE dt BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1], row[2])
				signins.append(signin)
				
		finally:
			cur.close()
			con.close()
			return signins
	
	@staticmethod
	def select_by_event_datetime(event_dt):
		''' Return the list of Signins associated with a given Event. '''
		signins = []
		
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (isoformat(event_dt),)
			cur.execute('SELECT * FROM signins WHERE eventdt=?', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1], row[2])
				signins.append(signin)
				
		finally:
			cur.close()
			con.close()
			return signins
	
	@staticmethod
	def select_by_all(id='*', start_dt='*', end_dt='*', event_dt='*'):
		''' Return a list of Signins using any combination of filters. '''
		signins = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		if type(event_dt == datetime):
			event_dt = isoformat(event_dt)
			
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (id, start_dt, end_dt, event_dt,)
			cur.execute('''SELECT * FROM signins WHERE student=? INTERSECT
			 SELECT * FROM signins WHERE dt BETWEEN ? AND ? INTERSECT 
			 SELECT * FROM signins WHERE eventdt=?''', symbol)
			for row in cur.fetchall():
				signin = Signin(row[0], row[1], row[2])
				signins.append(signin)
				
		finally:
			cur.close()
			con.close()
			return signins
	
	def __init__(self, dt, event_dt, s):
		self.signin_dt = dt			# a datetime object
		self.event_dt = event_dt	# a datetime object or 'NULL'
		self.student = s			# RFID number
	
	def __del__(self):
		self.delete()
		
	def update(self):
		''' Update an existing Signin record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.signin_dt, self.event_dt, self.student, self.event_dt, self.student,)
			cur.execute('''UPDATE signins 
			SET dt=?, eventdt=?, student=? 
			WHERE dt=? AND student=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Signin to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.signin_dt, self.event_dt, self.student,)
			cur.execute('INSERT OR ABORT INTO signins VALUES (?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Signin from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.signin_dt, self.student,)
			cur.execute('DELETE FROM signins WHERE dt=? AND student=?', symbol)
				
		finally:
			cur.close()
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
	def select_by_name(name):
		''' Return the list of Events of a given name. '''
		events = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (name,)
			cur.execute('SELECT * FROM events WHERE eventname=?', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		finally:
			cur.close()
			con.close()
			return events
	
	@staticmethod
	def select_by_datetime(start_dt, end_dt):
		''' Return the list of Events in a given datetime range. '''
		events = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (start_dt, end_dt,)
			cur.execute('SELECT * FROM events WHERE dt BETWEEN ? AND ?', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		finally:
			cur.close()
			con.close()
			return events
	
	@staticmethod
	def select_by_type(type):
		''' Return the list of Events of a given type. '''
		events = []
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (type,)
			cur.execute('SELECT * FROM events WHERE eventtype=?', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		finally:
			cur.close()
			con.close()
			return events
	
	@staticmethod
	def select_by_all(name, start_dt, end_dt, type):
		''' Return a list of Events using any combination of filters. '''
		events = []
		
		if type(start_dt == datetime):
			start_dt = isoformat(start_dt)
		if type(end_dt == datetime):
			end_dt = isoformat(end_dt)
		
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (name, start_dt, end_dt, type,)
			cur.execute('''SELECT * FROM events WHERE eventname=? INTERSECT 
			SELECT * FROM events WHERE dt BETWEEN ? AND ? INTERSECT 
			SELECT * FROM events WHERE eventtype=?''', symbol)
			for row in cur.fetchall():
				event = Event(row[0], row[1], row[2])
				events.append(event)
				
		finally:
			cur.close()
			con.close()
			return events
	
	def __init__(self, name, dt, t):
		self.event_name = name
		self.event_dt = dt	# a datetime object, primary key
		self.event_type = t	# One of the Event.TYPE_ constants 
		self.signins = []
		self.excuses = []
	
	def __del__(self):
		self.delete()
		
	def fetch_signins(self):
		''' Fetch all Signins for this Event from the database. '''
		self.signins = Signin.select_by_datetime(self.event_dt+Event.ATTENDANCE_OPENS, self.event_dt+Event.ATTENDANCE_CLOSES)
	
	def fetch_excuses(self):
		''' Fetch all Excuses for this Event from the database. '''
		self.excuses = Excuse.select_by_datetime(self.event_dt+Excuse.EXCUSES_OPENS, self.event_dt+Excuse.EXCUSES_CLOSES)
	
	def update(self):
		''' Update an existing Event record in the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.name, self.event_dt, self.event_type, self.event_dt,)
			cur.execute('''UPDATE events 
			SET eventname=?, dt=?, type=? 
			WHERE dt=?''', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def insert(self):
		''' Write the Event to the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			(self.name, self.event_dt, self.event_type,)
			cur.execute('INSERT INTO events VALUES (?,?,?)', symbol)
				
		finally:
			cur.close()
			con.close()
	
	def delete(self):
		''' Delete the Event from the DB. '''
		try:
			(con, cur) = gcdb.con_cursor()
			
			symbol = (self.event_dt,)
			cur.execute('DELETE FROM events WHERE dt=?', symbol)
				
		finally:
			cur.close()
			con.close()

	