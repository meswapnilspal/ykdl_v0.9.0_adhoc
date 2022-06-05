
package com.kc;

import java.util.Comparator;

/**
 * @author lw380
 *
 */
public class CategoryTable {

	long pseq;
	String id;
	long category_seq;
	String name;

	// empList.add(new Employee(0,"kimberly2",3920,"rakass"));

	public CategoryTable(long pseq, String id, long category_seq, String name) {
		// TODO Auto-generated constructor stub
		this.pseq = pseq; // pseq
		this.id = id; // id
		this.category_seq = category_seq; // category_seq
		this.name = name; // name
	}

	public long getpseq() {
		return pseq;
	}

}

class RecordComp implements Comparator<CategoryTable> {

	public int compare(CategoryTable r1, CategoryTable r2) {

		if (r1.getpseq() == r2.getpseq()) {
			return 0;
		} else if (r1.getpseq() > r2.getpseq()) {
			return 1;
		} else
			return -1;
	}
}
