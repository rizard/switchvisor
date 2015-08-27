package net.floodlightcontroller.switchvisor;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Maintain an ordered and unique collection of items. This
 * is essentially a blend of List and Set where the List must
 * now contain unique elements and the Set must maintain a
 * specified order of its elements.
 * 
 * We guarantee each item in the list is unique and that the
 * order of the items is preserved.
 * 
 * @author Ryan Izard, ryan.izard@bigswitch.com, rizard@g.clemson.edu
 *
 * @param the type of item to maintain
 */
public class OrderedSet<T> implements Iterable<T> {
	private final ArrayList<T> items;

	/*
	 * Constructors
	 */
	
	/**
	 * Create a new UniqueElementList. Defaults to empty.
	 */
	public OrderedSet() {
		this.items = new ArrayList<T>();
	}
	/**
	 * Create a new UniqueElementList of initial capacity 'capacity'.
	 * @param capacity
	 */
	public OrderedSet(int capacity) {
		this.items = new ArrayList<T>(capacity);
	}
	/**
	 * Create a new UniqueElementList from an ArrayList<T>. The
	 * list must have unique elements already.
	 * @param list
	 */
	public OrderedSet(ArrayList<T> list) {
		ensureContainsUniqueElements(list);
		this.items = new ArrayList<T>(list);
	}

	public boolean add(T o) {
		/*
		 * The future next element position on the list is the
		 * present size. So, we can use the size of the list
		 * to insert some item at the end (to avoid duplication
		 * of code).
		 */
		return add(this.items.size(), o);
	}
	
	public boolean add(int index, T o) {
		if (this.items.contains(o)) {
			return false;
		} else {
			this.items.add(index, o);
			return true;
		}
	}
	
	public boolean remove(T o) {
		return this.items.remove(o);
	}
	
	public T remove(int index) {
		return this.items.remove(index);
	}
	
	public boolean contains(T o) {
		return this.items.contains(o);
	}
	
	public boolean isEmpty() {
		return this.items.isEmpty();
	}
	
	public int size() {
		return this.items.size();
	}
	
	public boolean replace(T newObject, T oldObject) {
		if (!this.items.contains(oldObject) || this.items.contains(newObject)) {
			return false;
		} else {
			for (int i = 0; i < this.items.size(); i++) {
				T o = this.items.get(i);
				if (o.equals(oldObject)) {
					this.items.remove(i); // shifts down/left after remove
					this.items.add(i, newObject); // shifts up/right before add
					return true;
				}
			}
		}
		return false;
	}
	
	public T get(int index) {
		return this.items.get(index);
	}
	
	public Iterator<T> iterator() {
		return this.items.iterator();
	}

	/*
	 * Helper functions
	 */
	
	private void ensureContainsUniqueElements(ArrayList<T> list) {
		for (int i = 0; i < list.size(); i++) {
			for (int j = 0; j < list.size(); j++) {
				if (i != j && list.get(i).equals(list.get(j))) {
					throw new IllegalArgumentException("List must contain unique elements, but item " 
							+ list.get(i).toString() + " exists at positions " + i + " and " + j);
				}
			}
		}
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((items == null) ? 0 : items.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		@SuppressWarnings("unchecked")
		OrderedSet<T> other = (OrderedSet<T>) obj;
		if (items == null) {
			if (other.items != null)
				return false;
		} else if (!items.equals(other.items))
			return false;
		return true;
	}	
}