#include <cassert>
#include <algorithm>
#include "QueryProcessor.h"
#include "Table.h"
#include "Index.h"
#include "Iterator.h"
#include "Row.h"
#include "ColumnSelector.h"
#include "Operators.h"
#include "util.h"
#include "RowCompare.h"

//----------------------------------------------------------------------

// TableIterator 

unsigned TableIterator::n_columns() 
{
	return _table->columns().size();
}

void TableIterator::open() 
{
	_end = _table->rows().end();
	_input = _table->rows().begin();
}

Row* TableIterator::next() 
{
	if (_input != _end) {
		_input++;
		return *(_input - 1);
	}
	else
		return NULL;
}

void TableIterator::close() 
{
	_input = _end;
}

TableIterator::TableIterator(Table* table)
    : _table(table)
{
}

//----------------------------------------------------------------------

// IndexScan

unsigned IndexScan::n_columns()
{
	return _index->n_columns();
}

void IndexScan::open()
{
	_input = _index->lower_bound(*_lo);
	_end = _index->upper_bound(*_hi);
}

Row* IndexScan::next()
{
	if (_input != _end) {
		Row* row = _input->second;
		_input++;
		return row;
	}
	else
		return NULL;
}

void IndexScan::close()
{
	_input = _end;
}

IndexScan::IndexScan(Index* index, Row* lo, Row* hi)
    : _index(index),
      _lo(lo),
      _hi(hi == NULL ? lo : hi)
{}

//----------------------------------------------------------------------

// Select

unsigned Select::n_columns()
{
	return _input->n_columns();
}

void Select::open()
{
	_input->open();
}

Row* Select::next()
{
	Row* next = _input->next();
	while (next != NULL && !_predicate(next)) {
		Row::reclaim(next);
		next = _input->next();
	}
	return next;
}

void Select::close()
{
	_input->close();
}

Select::Select(Iterator* input, RowPredicate predicate)
    : _input(input),
      _predicate(predicate)
{
}

Select::~Select()
{
    delete _input;
}

//----------------------------------------------------------------------

// Project

unsigned Project::n_columns()
{
    return _column_selector.n_selected();
}

void Project::open()
{
    _input->open();
}

Row* Project::next()
{
    Row* projected = NULL;
    Row* row = _input->next();
	if (row) {
		projected = new Row();
		for (unsigned i = 0; i < _column_selector.n_selected(); i++) {
			projected->append(row->at(_column_selector.selected(i)));
		}
		Row::reclaim(row);
	}
    return projected;
}

void Project::close()
{
    _input->close();
}

Project::Project(Iterator* input, const initializer_list<unsigned>& columns)
    : _input(input),
      _column_selector(input->n_columns(), columns)
{}

Project::~Project()
{
    delete _input;
}

//----------------------------------------------------------------------

// NestedLoopsJoinIterator

unsigned NestedLoopsJoin::n_columns()
{
	return _left->n_columns() + _right->n_columns() - _left_join_columns.n_selected();
}

void NestedLoopsJoin::open()
{
	_left->open();
	_right->open();
	_left_row = _left->next();
	joinResult = new Row();
}

Row* NestedLoopsJoin::next()
{
	// Loop exits when:
	//      1. We found a tuple in s (second table) that joins with r (first table)
	//      2. s join r is done (returns NULL)

	while (1) {
		Row* _right_row = _right->next();

		if (_right_row == NULL) {       // The row in r (first table) has joined with every row in s (second table)
			_left_row = _left->next();  
			if (_left_row == NULL) {    // We have joined all rows in two tables
				Row::reclaim(_right_row);
				return NULL;
			}
			// restart s (second table) from the beginning
			_right->close();
			_right->open();
			_right_row = _right->next();
		}

		if (_right_row == NULL)         // Right is empty
			return NULL;

		if (_left_row == NULL) {        // Left is empty
			Row::reclaim(_right_row);
			return NULL;
		}         
                                        
		// Check whether equal or not   // Normal cases
		bool isEqual = true;
		for (unsigned i = 0; i < _left_join_columns.n_selected(); i++)
			if (_left_row->at(_left_join_columns.selected(i)) != _right_row->at(_right_join_columns.selected(i)))
				isEqual = false;
		if (isEqual) {
			*joinResult = *_left_row;
			for (unsigned i = 0; i < _right_join_columns.n_unselected(); i++)
				joinResult->append(_right_row->at(_right_join_columns.unselected(i)));
			Row::reclaim(_right_row);
			return joinResult;
		}
		else
			Row::reclaim(_right_row);
	}
}

void NestedLoopsJoin::close()
{
	_left->close();
	_right->close();
	delete joinResult;
}

NestedLoopsJoin::NestedLoopsJoin(Iterator* left,
	const initializer_list<unsigned>& left_join_columns,
	Iterator* right,
	const initializer_list<unsigned>& right_join_columns)
	: _left(left),
	_right(right),
	_left_join_columns(left->n_columns(), left_join_columns),
	_right_join_columns(right->n_columns(), right_join_columns),
	_left_row(NULL)
{
	assert(_left_join_columns.n_selected() == _right_join_columns.n_selected());
}

NestedLoopsJoin::~NestedLoopsJoin()
{
	delete _left;
	delete _right;
	Row::reclaim(_left_row);
}

//----------------------------------------------------------------------

// Sort

unsigned Sort::n_columns() 
{
	return _input->n_columns();
}

void Sort::open() 
{
	// initialize
	_input->open();
	Row* row = _input->next();
	while (row) {
		_sorted.emplace_back(row);
		row = _input->next();
	}

	// compare
	RowCompare* rcmp = new RowCompare(_sort_columns);
	sort(_sorted.begin(), _sorted.end(), *rcmp);
	_sorted_iterator = _sorted.begin();
	delete rcmp;
}

Row* Sort::next() 
{
	if (_sorted_iterator != _sorted.end())
		return *_sorted_iterator++;
	else
		return NULL;
}

void Sort::close() 
{
	_input->close();
	_sorted.clear();

}

Sort::Sort(Iterator* input, const initializer_list<unsigned>& sort_columns)
    : _input(input),
      _sort_columns(sort_columns)
{}

Sort::~Sort()
{
    delete _input;
}

//----------------------------------------------------------------------

// Unique

unsigned Unique::n_columns()
{
	return _input->n_columns();
}

void Unique::open() 
{
	_last_unique = new Row();
	_input->open();
}

Row* Unique::next()
{
	Row* row = _input->next();
	while (1) {
		if (row == NULL) {                           // empty or last
			Row::reclaim(row);
			return NULL;
		}

		if (row != NULL && _last_unique->empty()) {  // first
			*_last_unique = *row;
			return row;
		}

		for (unsigned i = 0; i < _input->n_columns(); i++) {
			if (row->at(i) != _last_unique->at(i)) {
				*_last_unique = *row;
				return row;
			}
		}
		*_last_unique = *row;
		Row::reclaim(row);
		row = _input->next();
	}
}

void Unique::close() 
{
	delete _last_unique;
	_input->close();
}

Unique::Unique(Iterator* input)
    : _input(input),
      _last_unique(NULL)
{}

Unique::~Unique()
{
    delete _input;
}
