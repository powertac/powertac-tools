package org.powertac.windpark;

import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;

@XStreamAlias("rmse_curve")
public class WindSpeedRMSE {
	@XStreamAlias("rmse")
	public static class RmseVal implements Comparable<RmseVal> {
		@XStreamAlias("hour")
		@XStreamAsAttribute
		private int hour = 0;
		
		@XStreamAlias("value")
		@XStreamAsAttribute
		private float value = 0;
		
		public RmseVal(int h, float v) {
			this.hour = h;
			this.value = v;
		}
		
		public int getHour() {
			return hour;
		}

		public float getValue() {
			return value;
		}

		public int compareTo(RmseVal o) {
			if (this.hour < o.hour) {
				return -1;
			} else if (this.hour > o.hour) {
				return 1;
			}
			return 0;
		}
		
		
	}
	
	@XStreamImplicit	
	private SortedSet<RmseVal> rmseVals = new TreeSet<RmseVal>();
	
	public WindSpeedRMSE () {}
	
	public WindSpeedRMSE(SortedSet<RmseVal> vals) {
		this.rmseVals.addAll(vals);
	}
	
	public Set<RmseVal> getRmseVals() {
		return Collections.unmodifiableSortedSet(this.rmseVals);
	}
	
	public void addRmseVal(RmseVal val) {
		this.rmseVals.add(val);
	}
	
	public void addRmseVal(int hr, float val) {
		this.rmseVals.add(new RmseVal(hr, val));
	}
	
	public float getValue(int hr) {
		float val = 0;
		for (RmseVal rv : rmseVals) {
			if (rv.getHour() == hr) {
				val = rv.getValue();
				break;
			}
		}
		
		return val;
	}
}
