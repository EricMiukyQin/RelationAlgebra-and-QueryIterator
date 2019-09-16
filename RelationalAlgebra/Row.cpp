#include "Database.h"
#include "util.h"

bool Row::operator==(const Row &that) const
{
	ColumnNames originalColumnNames = _table->columns();
	ColumnNames testColumnNames = that.table()->columns();
	if (originalColumnNames.size() != testColumnNames.size())          // Number of columns are not equal
		return false;
	else {
		for (unsigned int i = 0; i < originalColumnNames.size(); i++)
			if (testColumnNames[i] != originalColumnNames[i])          // Columns names are not equal
				return false;
	}
    return true;
}

bool Row::operator!=(const Row& row) const
{
    return !operator==(row);
}

const Table *Row::table() const
{
    return _table;
}

const vector<string> &Row::data() const
{
	return _values;
}

const string &Row::value(const string &column) const
{
	int pos = _table->columns().position(column);
	// Check if value found or not
	if (pos != -1)
		return _values[pos];
	else
		throw TableException("Value not found");
	//return *new string("");
}

const string &Row::at(unsigned i) const
{
	if (i >= 0 && i <= _values.size() - 1)
		return _values[i];
	else
		throw TableException("There is no certain column in this row");
}

void Row::append(const string &value)
{
	// Check if number of columns is out of bound before adding new data
	if (_table->columns().size() > _values.size()) {
		_values.push_back(value);
	}
	else
		throw TableException("Too many columns to add");
}

unsigned long Row::size() const
{
	return _values.size();
}

Row::Row(const Table *table)
        : _table(table)
{}

Row::~Row()
{
	vector<string>().swap(_values);
	_values.clear();
}