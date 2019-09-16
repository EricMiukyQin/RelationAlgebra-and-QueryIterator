#include <sstream>
#include <cstring>
#include "RelationalAlgebra.h"
#include "Database.h"
#include "unittest.h"

bool unionCompatible(Table *r, Table *s);
bool disjointColumnsNames_Check(Table *r, Table *s);
NameMap namemapCreate(Table *t);
bool addRow(Table *table, const vector<string>& values);

//Union-compatible detect
bool unionCompatible(Table *r, Table *s)
{
	if (r->columns().size() != s->columns().size())
		return false;
	else
		return true;
	/*else {
		set<string> check;
		for (unsigned int i = 0; i < r->columns().size(); i++)
			check.insert(r->columns()[i]);
		for (unsigned int i = 0; i < s->columns().size(); i++)
			if (check.insert(s->columns()[i]).second == true)
				return false;
		return true;
	}*/
}

//Disjoint columnsNames detect
bool disjointColumnsNames_Check(Table *r, Table *s)
{
	vector<string> rColumnsName = r->columns();
	vector<string> sColumnsName = s->columns();
	for (unsigned int i = 0; i < rColumnsName.size(); i++)
		for (unsigned int j = 0; j < sColumnsName.size(); j++)
			if (rColumnsName[i] == sColumnsName[j])
				return false;
	return true;
}

// Create NameMap
NameMap namemapCreate(Table *t)
{
	NameMap result;
	for (unsigned int i = 0; i < t->columns().size(); i++)
		result.emplace(t->columns()[i], t->name() + t->columns()[i]);
	return result;
}

// Add vector<string> directly to table
bool addRow(Table *table, const vector<string>& values)
{
	Row* row = new Row(table);
	try {
		unsigned long n = values.size();
		for (unsigned i = 0; i < n; i++) {
			row->append(values.at(i));
		}
		bool added = table->add(row);
		if (!added) {
			delete row;
		}
		return added;
	}
	catch (TableException& e) {
		delete row;
		throw e;
	}
}

Table *onion(Table *r, Table *s)
{
	// Check if union compatible
	if (!unionCompatible(r, s))
		throw UnionIncompatibilityException("Tables have different columns.");

	Table *result = Database::new_table(Database::new_table_name(), r->columns());
	set<Row*, RowCompare>::iterator it;

	// Correct cases
	for (it = r->rows().begin(); it != r->rows().end(); it++)
		addRow(result, (*it)->data());
	for (it = s->rows().begin(); it != s->rows().end(); it++)
		addRow(result, (*it)->data());
	return result;
}

Table *intersect(Table *r, Table *s)
{
	// Check if union compatible
	if (!unionCompatible(r, s))
		throw UnionIncompatibilityException("Two tables are not union compatible");

	Table* result = Database::new_table(Database::new_table_name(), r->columns());
	set<Row*, RowCompare>::iterator it;
	RowSet sRows = s->rows();

	// Correct cases
	for (it = r->rows().begin(); it != r->rows().end(); it++)
		if (sRows.find(*it) != sRows.end())
			addRow(result, (*it)->data());
	return result;
}

Table *diff(Table *r, Table *s)
{
	// Check if union compatible
	if (!unionCompatible(r, s))
		throw UnionIncompatibilityException("Two tables are not union compatible");

	Table* result = Database::new_table(Database::new_table_name(), r->columns());
	set<Row*, RowCompare>::iterator it;
	RowSet sRows = s->rows();

	// Correct cases
	for (it = r->rows().begin(); it != r->rows().end(); it++)
		if (sRows.find(*it) == sRows.end())
			addRow(result, (*it)->data());
	return result;
}

Table *product(Table *r, Table *s)
{
	// Check if columns are disjoint
	if (!disjointColumnsNames_Check(r, s))
		throw TableException("input tables do not have disjoint column names");

	Table *r_rename = rename(r, namemapCreate(r));
	Table *s_rename = rename(s, namemapCreate(s));
	ColumnNames newColumnNames = r_rename->columns();
	set<Row*, RowCompare>::iterator it_r;
	set<Row*, RowCompare>::iterator it_s;

	// New table and columnnames
	newColumnNames.insert(newColumnNames.end(), s_rename->columns().begin(), s_rename->columns().end());
	Table* result = Database::new_table(Database::new_table_name(), newColumnNames);

	// Correct cases
	vector<string> temp;
	for (it_r = r_rename->rows().begin(); it_r != r_rename->rows().end(); it_r++) {
		for (it_s = s_rename->rows().begin(); it_s != s_rename->rows().end(); it_s++) {
			vector<string>().swap(temp);
			temp.clear();
			temp.insert(temp.end(), ((*it_r)->data()).begin(), ((*it_r)->data()).end());
			temp.insert(temp.end(), ((*it_s)->data()).begin(), ((*it_s)->data()).end()); // create new row combined with two rows
			addRow(result, temp);
		}
	}
	return result;
}

Table *rename(Table *r, const NameMap &name_map)
{
	ColumnNames rColoumnNames = r->columns();

	// Check if renaming a column more than once
	set<string> test;
	for (auto it = name_map.begin(); it != name_map.end(); it++)
		test.insert(it->first);

	// Correct cases
	if (rColoumnNames.size() != name_map.size())
		throw TableException("Too few columns to rename");
	else if (test.size() != name_map.size())
		throw TableException("Can not rename a column more than once");
	else {
		for (auto it = name_map.begin(); it != name_map.end(); it++) {
			bool isFound = false;
			for (unsigned int i = 0; i < rColoumnNames.size(); i++) {
				if (it->first == rColoumnNames[i]) {
					isFound = true;
					rColoumnNames[i] = it->second;
				}
			}
			if (!isFound)
				throw TableException("Can not rename columns that don't exist");
		}
	}
	Table* result = Database::new_table(Database::new_table_name(), rColoumnNames);
	set<Row*, RowCompare>::iterator it_set;
	for (it_set = r->rows().begin(); it_set != r->rows().end(); it_set++)
		addRow(result, (*it_set)->data());
	return result;
}

Table *select(Table *r, RowPredicate predicate)
{
	Table* result = Database::new_table(Database::new_table_name(), r->columns());
	set<Row*, RowCompare>::iterator it;
	for (it = r->rows().begin(); it != r->rows().end(); it++)
		if (predicate(*it))
			addRow(result, (*it)->data());
	return result;
}

Table *project(Table *r, const ColumnNames &columns)
{

	// Check if columns is empty
	if (columns.empty()) {
		throw TableException("Columns is empty");
		return Database::new_table(Database::new_table_name(), r->columns());
	}

	// Check if columns is not existed
	set<string> test(r->columns().begin(), r->columns().end());
	for (unsigned int i = 0; i < columns.size(); i++)
		if (test.find(columns[i]) == test.end()) {
			throw TableException("Can not refer to a column that does not exist");
			return Database::new_table(Database::new_table_name(), r->columns());
		}

	// Correct cases
	Table* result = Database::new_table(Database::new_table_name(), columns);
	set<Row*, RowCompare>::iterator it;
	for (it = r->rows().begin(); it != r->rows().end(); it++) {
		vector<string> *temp = new vector<string>;
		for (unsigned int i = 0; i < columns.size(); i++)
			temp->push_back((*it)->value(columns[i]));
		addRow(result, *temp);
		delete temp;
	}
	return result;
}

Table *join(Table *r, Table *s)
{
	// Get join columns and same columns
	ColumnNames joinColumns_for_s;
	vector<string> sameColumns;
	unsigned int count = 0;

	for (unsigned j = 0; j < s->columns().size(); j++) {
		bool isSame = false;
		for (unsigned i = 0; i < r->columns().size(); i++) {
			if (s->columns()[j] == r->columns()[i]) {
				isSame = true;
				sameColumns.push_back(s->columns()[j]);
				count++;
				break;
			}
		}
		if (!isSame)
			joinColumns_for_s.push_back(s->columns()[j]);
	}

	// Check if have no columns in common
	if (count == 0) {
		throw JoinException("Have no columns in common");
		return Database::new_table(Database::new_table_name(), {});
	}

	// Correct cases
	ColumnNames newColumnsName = r->columns();
	newColumnsName.insert(newColumnsName.end(), joinColumns_for_s.begin(), joinColumns_for_s.end());
	Table* result = Database::new_table(Database::new_table_name(), newColumnsName);

	set<Row*, RowCompare>::iterator it_r;
	set<Row*, RowCompare>::iterator it_s;
	for (it_r = r->rows().begin(); it_r != r->rows().end(); it_r++) {
		for (it_s = s->rows().begin(); it_s != s->rows().end(); it_s++) {
			bool isAllsame = true;
			for (unsigned int i = 0; i < sameColumns.size(); i++) {
				if ((*it_r)->value(sameColumns[i]) == (*it_s)->value(sameColumns[i]) && isAllsame == true)
					continue;
				else {
					isAllsame = false;
					break;
				}
			}
			if (isAllsame) {
				vector<string> *temp = new vector<string>;
				temp->insert(temp->end(), (*it_r)->data().begin(), (*it_r)->data().end());
				for (unsigned int k = 0; k < joinColumns_for_s.size(); k++)
					temp->push_back((*it_s)->value(joinColumns_for_s[k]));
				addRow(result, *temp);
				delete temp;
			}
		}
	}
	return result;
}