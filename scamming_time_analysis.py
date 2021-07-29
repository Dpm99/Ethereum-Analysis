from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import time

class Scam(MRJob):


	scams={}

	def mapper_json(self):

		with open('scams.json') as f:

			json_line = json.load(f)
			in_lines = json_line['result']

			for line in in_lines:
				key_vals = in_lines[line]
				addresses = key_vals['addresses']
				id_ = key_vals['id']
				name = key_vals['name']
				category = key_vals['category']

				try:
					sub_cat = key_vals['subcategory']
				except:
					sub_cat=''
					continue

				status = key_vals['status']
				id_list = [id_, name, category, sub_cat, status]
				for address in addresses:
					id_list.append(address)
					self.scams[address] = id_list

	def mapper_repl_join(self, _, line):

		try:
			fields=line.split(',')
			if len(fields)==7:

				address = fields[2]
				value = int(fields[3])
				time_epoch = int(fields[6])
				date = time.strftime("%Y%m", time.gmtime(time_epoch))

				if address in self.scams:
					scam_info = self.scams[address]
					if "Scamming" in scam_info:
						yield((scam_info, date), value)
		except:
			pass


###################### ###########################
	def combiner_sum(self, key, values):
		scam_sum=sum(values)
		yield(key, scam_sum)

	def reducer_sum(self, key, values):
		scam_sum=sum(values)
		yield(key, scam_sum)

##################### ############################

	def mapper_sort(self, key, values):

		key2 = key[1]

		yield(key2, (values, key[0]))

		sorted_values = sorted(values, reverse=True, key=lambda x:x[0])

		yield(key, sorted_values[0])

	def reducer_sort(self, key, values):

		count=0
		sorted_values =sorted(values, reverse=True, key=lambda x:x[0])
		yield(key, sorted_values[0])

	def steps(self):

		return([MRStep(mapper_init=self.mapper_json, mapper=self.mapper_repl_join, combiner=self.combiner_sum, reducer=self.reducer_sum),
			MRStep(mapper=self.mapper_sort, combiner = self.combiner_sort, reducer=self.reducer_sort)])


if __name__=='__main__':

	Scam.run()
	def combiner_sort(self, key, values):

		count=0

		sorted_values = sorted(values, reverse=True, key=lambda x:x[0])

		yield(key, sorted_values[0])

	def reducer_sort(self, key, values):

		count=0
		sorted_values =sorted(values, reverse=True, key=lambda x:x[0])
		yield(key, sorted_values[0])

	def steps(self):

		return([MRStep(mapper_init=self.mapper_json, mapper=self.mapper_repl_join, combiner=self.combiner_sum, reducer=self.reducer_sum),
			MRStep(mapper=self.mapper_sort, combiner = self.combiner_sort, reducer=self.reducer_sort)])


if __name__=='__main__':

	Scam.run()
