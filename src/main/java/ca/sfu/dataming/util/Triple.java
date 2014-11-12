package ca.sfu.dataming.util;

/**
 * @author congzicun
 * @since 2014-11-10 11:38 AM
 * To change this template use File | Settings | File Templates.
 */

import java.io.Serializable;

/**
 * A generic class for pairs.
 *
 * @param <T1>
 * @param <T2>
 */
public class Triple<T1, T2, T3> implements Serializable {
    private static final long serialVersionUID = -3986244606585552569L;
    protected T1 first = null;
    protected T2 second = null;
    protected T3 third = null;

    /**
     * Default constructor.
     */
    public Triple() {
    }

    /**
     * Constructor
     *
     * @param a operand
     * @param b operand
     */
    public Triple(T1 a, T2 b, T3 c) {
        this.first = a;
        this.second = b;
        this.third = c;
    }

    /**
     * Constructs a new pair, inferring the type via the passed arguments
     *
     * @param <T1> type for first
     * @param <T2> type for second
     * @param a    first element
     * @param b    second element
     * @return a new pair containing the passed arguments
     */
    public static <T1, T2, T3> Triple<T1, T2, T3> newTriple(T1 a, T2 b, T3 c) {
        return new Triple<T1, T2, T3>(a, b, c);
    }

    /**
     * Replace the first element of the pair.
     *
     * @param a operand
     */
    public void setFirst(T1 a) {
        this.first = a;
    }

    /**
     * Replace the second element of the pair.
     *
     * @param b operand
     */
    public void setSecond(T2 b) {
        this.second = b;
    }

    /**
     * Replace the third element of the pair.
     *
     * @param c operand
     */
    public void setThird(T3 c) {
        this.third = c;
    }

    /**
     * Return the first element stored in the pair.
     *
     * @return T1
     */
    public T1 getFirst() {
        return first;
    }

    /**
     * Return the second element stored in the pair.
     *
     * @return T2
     */
    public T2 getSecond() {
        return second;
    }

    /**
     * Return the third element stored in the pair.
     *
     * @return T3
     */
    public T3 getThird() {
        return third;
    }

    private static boolean equals(Object x, Object y) {
        return (x == null && y == null) || (x != null && x.equals(y));
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean equals(Object other) {
        return other instanceof Triple && equals(first, ((Triple) other).first) &&
                equals(second, ((Triple) other).second);
    }

    @Override
    public int hashCode() {
        if (first == null) {
            if (second == null)
                return third == null ? 0 : third.hashCode() + 1;
            else
                return second.hashCode() + 1;
        } else if (second == null)
            return first.hashCode() + 4 + third.hashCode();
        else
            return first.hashCode() * 17 * 17 + second.hashCode() * 17 + third.hashCode();
    }

    @Override
    public String toString() {
        return "{" + getFirst() + "," + getSecond() + "}";
    }
}

