#include <cstring>
#include "Database.h"

using namespace std;

const string &Table::name() const
{
    return _name;
}

const ColumnNames &Table::columns() const
{
    return _columns;
}

const RowSet& Table::rows()
{
	return _rows;
    //return *new RowSet();
}

bool Table::add(Row* row)
{
	// Check if number of columns in new row is out of bound
	if (row->data().size() > _columns.size()) {
		throw TableException("Add row with too many columns");
		return false;
	}
	else if (row->data().size() < _columns.size()) {
		throw TableException("Add row with too few columns");
		return false;
	}
	else {
		pair<set<Row*, RowCompare>::iterator, bool> ret;
		ret = _rows.insert(row);
		if (ret.second == false)
			return false;
		else
			return true;
	}
}

bool Table::remove(Row* row)
{
	if (row->data().size() == _columns.size()) {
		set<Row*, RowCompare>::iterator it;
		it = _rows.find(row);
		if (it != _rows.end()) {             // Matching row is found
			delete *it;
			_rows.erase(it);                 // Remove the matching row
			return true;
		}
		else
			return false;
	}
	else
		throw TableException("Can not remove incompatible row");

}

bool Table::has(Row* row)
{
	if (row->data().size() == _columns.size())
		if (_rows.find(row) != _rows.end())               // Matching row is found
			return true;
		else
			return false;
	else
		throw TableException("Can not have bad row");
}

Table::Table(const string &name, const ColumnNames &columns)
    : _name(name),
      _columns(columns)
{
	// Check whether columns are empty or not
	if (!columns.empty()) {
	    // Check whether any duplicate column name in given set or not
		set<string> columnSet(_columns.begin(), _columns.end());
		if (columnSet.size() < _columns.size())
			throw TableException("Duplicate columns");
	}
	else
		throw TableException("No columns");
}

Table::~Table()
{
	set<Row*, RowCompare>::iterator it;
	for (it = _rows.begin(); it != _rows.end(); it++) {
		delete *it;
	}
	_rows.clear();
}