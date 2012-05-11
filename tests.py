import os
import unittest
from datetime import *
import apsw
from gc_attendance import *

class AttendanceTestCase(unittest.TestCase):
	def setUp(self):
		unittest.TestCase.setUp(self)
		from gc_attendance import *
		self.db = AttendanceDB(os.path.join(os.getcwd(), 'dbtests.sqlite'))
		self.glee_club = Organization('Glee Club')
		self.shm = Organization('SHM')
		self.av = Organization('Alden Voices')
		self.a_term = Term('A11', date(2011, 8, 25), date(2011, 10, 13), [date(2011, 9, 5)])
		self.b_term = Term('B11', date(2011, 10, 25), date(2011, 12, 15), [date(2011, 11, 23), date(2011, 11, 24), date(2011, 11, 25)])
		self.fall_semester = Semester('fall_2011', self.a_term, self.b_term)
		self.gc_group = Group(None, self.glee_club, self.fall_semester)
		self.shm_group = Group(None, self.shm, self.fall_semester)
		self.student10000 = Student(10000, 'Shawn', 'Onessimo', 'smonessimo@wpi.edu')
		self.student11262 = Student(11262, 'Andrew', 'St. Jean', 'astjean@wpi.edu')
		self.student12345 = Student(12345, 'Ryan', 'Staunch', 'rstaunch@wpi.edu')
		self.student11959 = Student(11959, 'John', 'Delorey', 'jfd@wpi.edu')
		self.student12011 = Student(12011, 'Tony', 'Guerra', 'aguerra@wpi.edu')
		self.student42000 = Student(42000, 'Jose', 'Navedo', 'jnavedo@wpi.edu')
		self.student56789 = Student(56789, 'Travis', 'Briggs', 'tbriggs@wpi.edu')
		self.student42737 = Student(42737, 'Joe', 'Baker', 'jbaker@alum.wpi.edu')
		self.student24631 = Student(24631, 'Anika', 'Blodgett', 'ablodgett@wpi.edu')
		
		self.rehearsal1 = Event(None, 'Rehearsal', datetime(2011, 9, 6, 18, 30), Event.TYPE_REHEARSAL, self.gc_group, self.fall_semester)
		self.signin_r1_10000 = Signin(datetime(2011, 9, 6, 18, 28), None, self.student10000)
		self.signin_r1_11262 = Signin(datetime(2011, 9, 6, 18, 28), None, self.student11262)
		self.signin_r1_12345 = Signin(datetime(2011, 9, 6, 18, 30), None, self.student12345)
		self.signin_r1_11959 = Signin(datetime(2011, 9, 6, 18, 32), None, self.student11959)
		self.signin_r1_12011 = Signin(datetime(2011, 9, 6, 18, 31), None, self.student12011)
		self.signin_r1_42000 = Signin(datetime(2011, 9, 6, 18, 34), None, self.student42000)
		self.signin_r1_56789 = Signin(datetime(2011, 9, 6, 20, 12), None, self.student56789)
		self.signin_r1_42737 = Signin(datetime(2011, 9, 6, 18, 35), None, self.student42737)
		
		self.dress1 = Event(None, 'Rehearsal for September 11', datetime(2011, 9, 10, 10, 00), Event.TYPE_DRESS, self.gc_group, self.fall_semester)
		self.concert1 = Event(None, 'Memorial Mass', datetime(2011, 9, 11, 12, 00), Event.TYPE_CONCERT, self.gc_group, self.fall_semester)
		self.rehearsal2 = Event(None, 'Rehearsal', datetime(2011, 9, 13, 18, 30), Event.TYPE_REHEARSAL, self.gc_group, self.fall_semester)
		self.signin_r2_10000 = Signin(datetime(2011, 9, 13, 18, 28), None, self.student10000)
		self.signin_r2_11262 = Signin(datetime(2011, 9, 13, 18, 28), None, self.student11262)
		self.signin_r2_12345 = Signin(datetime(2011, 9, 13, 18, 30), None, self.student12345)
		self.signin_r2_11959 = Signin(datetime(2011, 9, 13, 18, 47), None, self.student11959)
		self.signin_r2_12011 = Signin(datetime(2011, 9, 13, 18, 31), None, self.student12011)
		self.signin_r2_42000 = Signin(datetime(2011, 9, 13, 18, 34), None, self.student42000)
		self.absence_r2_56789 = Absence(self.student56789, Absence.TYPE_PENDING, self.rehearsal2)
		self.absence_r2_42737 = Absence(self.student42737, Absence.TYPE_PENDING, self.rehearsal2)
		self.excuse_r2_42737 = Excuse(None, datetime(2011, 9, 13, 11, 37), self.rehearsal2, 'oh god rbe ahhhh', self.student42737)
		
		self.rehearsal3 = Event(None, 'Rehearsal', datetime(2011, 9, 20, 18, 30), Event.TYPE_REHEARSAL, self.gc_group, self.fall_semester)
		self.rehearsal4 = Event(None, 'Rehearsal', datetime(2011, 9, 27, 18, 30), Event.TYPE_REHEARSAL, self.gc_group, self.fall_semester)
		self.rehearsal5 = Event(None, 'Rehearsal', datetime(2011, 10, 4, 18, 30), Event.TYPE_REHEARSAL, self.gc_group, self.fall_semester)
		self.rehearsal6 = Event(None, 'Rehearsal', datetime(2011, 10, 11, 18, 30), Event.TYPE_REHEARSAL, self.gc_group, self.fall_semester)
	
	def test_database(self):
		''' Basic database functionality tests.
		Insert an object into the DB, query for it, compare the two, etc. '''
		#try:
		from gc_attendance import *
		import apsw
		self.db.create_tables(self.db.memory)
		# Table tests
		tables = []
		cur = self.db.memory.cursor()
		cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
		for row in cur.fetchall():
			tables.append(row[0])
		cur.close()
		assert 'absences' in tables
		assert 'daysoff' in tables
		assert 'events' in tables
		assert 'excuses' in tables
		assert 'group_memberships' in tables
		assert 'groups' in tables
		assert 'organizations' in tables
		assert 'semesters' in tables
		assert 'signins' in tables
		assert 'students' in tables
		assert 'terms' in tables
		
		# Term tests
		self.a_term.insert(self.db.memory)
		self.b_term.insert(self.db.memory)
		selected_a_term = Term.select_by_name(self.a_term.name, self.db.memory)
		assert selected_a_term is not None
		selected_b_term = Term.select_by_name(self.b_term.name, self.db.memory)
		assert selected_b_term is not None
		# TODO: equality checks
		
		#finally:
			
		
	def tearDown(self):
		unittest.TestCase.tearDown(self)
		del self.db
		#os.remove(os.path.join(os.getcwd(), 'dbtests.sqlite'))

if __name__ == '__main__':
	unittest.main()	
