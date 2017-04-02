/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.math.impls.matrix;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import org.apache.ignite.math.*;
import org.apache.ignite.math.exceptions.*;
import org.apache.ignite.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.math.impls.vector.*;
import org.junit.*;

import static org.junit.Assert.*;

/**
 * Tests for {@link Matrix} implementations.
 */
public class MatrixImplementationsTest extends ExternalizeTest<Matrix> {
    /** */ private static final double DEFAULT_DELTA = 0.000000001d;

    /** */
    private static final Map<Class<? extends Matrix>, Class<? extends Vector>> typesMap = typesMap();

    /** */
    private void consumeSampleMatrix(BiConsumer<Matrix, String> consumer) {
        new MatrixImplementationFixtures().consumeSampleMatrix(null, consumer);
    }

    /** */
    @Test
    public void externalizeTest() {
        consumeSampleMatrix((m, desc) -> externalizeTest(m));
    }

    /** */
    @Test
    public void testLike() {
        consumeSampleMatrix((m, desc) -> {
            if (typesMap().containsKey(m.getClass())) {
                Matrix like = m.like(m.rowSize(), m.columnSize());

                assertEquals("Wrong \"like\" matrix for " + desc + "; Unexpected class: " + like.getClass().toString(),
                    like.getClass(),
                    m.getClass());
                assertEquals("Wrong \"like\" matrix for " + desc + "; Unexpected rows.", like.rowSize(), m.rowSize());
                assertEquals("Wrong \"like\" matrix for " + desc + "; Unexpected columns.", like.columnSize(), m.columnSize());
                assertEquals("Wrong \"like\" matrix for " + desc + "; Unexpected storage class: " + like.getStorage().getClass().toString(),
                    like.getStorage().getClass(),
                    m.getStorage().getClass());
            }
        });
    }

    /** */
    @Test
    public void testCopy() {
        consumeSampleMatrix((m, desc) -> {
            Matrix cp = m.copy();
            assertTrue("Incorrect copy for empty matrix " + desc, cp.equals(m));

            if (ignore(m.getClass()))
                return;

            fillMatrix(m);
            cp = m.copy();

            assertTrue("Incorrect copy for matrix " + desc, cp.equals(m));
        });
    }

    /** */ @Test
    public void testHaveLikeVector() throws InstantiationException, IllegalAccessException {
        for (Class<? extends Matrix> key : typesMap.keySet()) {
            Class<? extends Vector> val = typesMap.get(key);

            if (val == null && !ignore(key))
                System.out.println("Missing test for implementation of likeMatrix for " + key.getSimpleName());
        }
    }

    /** */
    @Test
    public void testLikeVector() {
        consumeSampleMatrix((m, desc) -> {
            if (typesMap().containsKey(m.getClass())) {
                Vector likeVector = m.likeVector(m.columnSize());

                assertNotNull(likeVector);
                assertEquals("Unexpected value for " + desc, likeVector.size(), m.columnSize());
            }
        });
    }

    /** */
    @Test
    public void testAssignSingleElement() {
        consumeSampleMatrix((m,desc) -> {
            if (ignore(m.getClass()))
                return;

            final double assignVal = Math.random();

            m.assign(assignVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i, j), assignVal) == 0);
        });
    }

    /** */
    @Test
    public void testAssignArray() {
        consumeSampleMatrix((m,desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = new double[m.rowSize()][m.columnSize()];

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = Math.random();

            m.assign(data);

            for (int i = 0; i < m.rowSize(); i++) {
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i, j), data[i][j]) == 0);
            }
        });
    }

    /** */
    @Test
    public void testAssignFunction() {
        consumeSampleMatrix((m,desc) -> {
            if (ignore(m.getClass()))
                return;

            m.assign((i, j) -> (double) (i * m.columnSize() + j));

            for (int i = 0; i < m.rowSize(); i++) {
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i, j), (double) (i * m.columnSize() + j)) == 0);
            }
        });
    }

    /** */
    @Test
    public void testPlus() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            double plusVal = Math.random();

            Matrix plus = m.plus(plusVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(plus.get(i, j), m.get(i, j) + plusVal) == 0);
        });
    }

    /** */
    @Test
    public void testPlusMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = fillAndReturn(m);

            Matrix plus = m.plus(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(plus.get(i, j), data[i][j] * 2.0) == 0);
        });
    }

    /** */
    @Test
    public void testMinusMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Matrix minus = m.minus(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(minus.get(i, j), 0.0) == 0);
        });
    }

    /** */
    @Test
    public void testTimes() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            double timeVal = Math.random();
            Matrix times = m.times(timeVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(times.get(i, j), m.get(i, j) * timeVal) == 0);
        });
    }

    /** */
    @Test
    public void testDivide() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] data = fillAndReturn(m);

            double divVal = Math.random();

            Matrix divide = m.divide(divVal);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(divide.get(i, j), data[i][j] / divVal) == 0);
        });
    }

    /** */
    @Test
    public void testTranspose() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Matrix transpose = m.transpose();

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i, j), transpose.get(j, i)) == 0);
        });
    }

    /** */
    @Test
    public void testDeterminant() {
        consumeSampleMatrix((m, desc) -> {
            if (m.rowSize() != m.columnSize())
                return;

            double[][] doubles = fillIntAndReturn(m);

            if (m.rowSize() == 1) {
                assertEquals("Unexpected value " + desc, m.determinant(), doubles[0][0], 0d);

                return;
            }

            if (m.rowSize() == 2) {
                double det = doubles[0][0] * doubles[1][1] - doubles[0][1] * doubles[1][0];
                assertEquals("Unexpected value " + desc, m.determinant(), det, 0d);

                return;
            }

            if (m.rowSize() > 512)
                return; // IMPL NOTE if row size >= 30000 it takes unacceptably long for normal test run.

            if (ignore(m.getClass()))
                return;

            Matrix diagMtx = m.like(m.rowSize(), m.columnSize());

            diagMtx.assign(0);
            for (int i = 0; i < m.rowSize(); i++)
                diagMtx.set(i, i, m.get(i, i));

            double det = 1;

            for (int i = 0; i < diagMtx.rowSize(); i++)
                det *= diagMtx.get(i, i);

            try {
                assertEquals("Unexpected value " + desc, det, diagMtx.determinant(), DEFAULT_DELTA);
            } catch (Exception e) {
                System.out.println(desc);
                throw e;
            }
        });
    }

    /** */
    @Test
    public void testMap() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            m.map(x -> 10d);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertTrue("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i, j), 10d) == 0);
        });
    }

    /** */
    @Test
    public void testMapMatrix() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            double[][] doubles = fillAndReturn(m);

            testMapMatrixWrongCardinality(m, desc);

            Matrix cp = m.copy();

            m.map(cp, (m1, m2) -> m1 + m2);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(i, j), doubles[i][j] * 2, 0d);
        });
    }

    /** */
    @Test
    public void testViewRow() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            for (int i = 0; i < m.rowSize(); i++) {
                Vector vector = m.viewRow(i);
                assert vector != null;

                for (int j = 0; j < m.columnSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(i, j), vector.get(j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testViewCol() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            for (int i = 0; i < m.columnSize(); i++) {
                Vector vector = m.viewColumn(i);
                assert vector != null;

                for (int j = 0; j < m.rowSize(); j++)
                    assertEquals("Unexpected value for " + desc + " at (" + i + "," + j + ")",
                        m.get(j, i), vector.get(j), 0d);
            }
        });
    }

    /** */
    @Test
    public void testFoldRow() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Vector foldRows = m.foldRows(Vector::sum);

            for (int i = 0; i < m.rowSize(); i++) {
                Double locSum = 0d;

                for (int j = 0; j < m.columnSize(); j++)
                    locSum += m.get(i, j);

                assertEquals("Unexpected value for " + desc + " at " + i,
                    foldRows.get(i), locSum, 0d);
            }
        });
    }

    /** */
    @Test
    public void testFoldCol() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            Vector foldCols = m.foldColumns(Vector::sum);

            for (int j = 0; j < m.columnSize(); j++) {
                Double locSum = 0d;

                for (int i = 0; i < m.rowSize(); i++)
                    locSum += m.get(i, j);

                assertEquals("Unexpected value for " + desc + " at " + j,
                    foldCols.get(j), locSum, 0d);
            }
        });
    }

    /** */
    @Test
    public void testSum() {
        consumeSampleMatrix((m, desc) -> {
            double[][] data = fillAndReturn(m);

            double sum = m.sum();

            double rawSum = 0;
            for (double[] anArr : data)
                for (int j = 0; j < data[0].length; j++)
                    rawSum += anArr[j];

            assertTrue("Unexpected value for " + desc,
                Double.compare(sum, rawSum) == 0);
        });
    }

    /** */
    @Test
    public void testMax() {
        consumeSampleMatrix((m, desc) -> {
            double[][] doubles = fillAndReturn(m);
            double max = Double.NEGATIVE_INFINITY;

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    max = max < doubles[i][j] ? doubles[i][j] : max;

            assertEquals("Unexpected value for " + desc, m.maxValue(), max, 0d);
        });
    }

    /** */
    @Test
    public void testMin() {
        consumeSampleMatrix((m, desc) -> {
            double[][] doubles = fillAndReturn(m);
            double min = Double.MAX_VALUE;

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    min = min > doubles[i][j] ? doubles[i][j] : min;

            assertEquals("Unexpected value for " + desc, m.minValue(), min, 0d);
        });
    }

    /** */
    @Test
    public void testGetElement() {
        consumeSampleMatrix((m, desc) -> {
            if (!(readOnly(m)))
                fillMatrix(m);

            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++) {
                    final Matrix.Element e = m.getElement(i, j);

                    final String details = desc + " at [" + i + "," + j + "]";

                    assertEquals("Unexpected element row " + details, i, e.row());
                    assertEquals("Unexpected element col " + details, j, e.column());

                    final double val = m.get(i, j);

                    assertEquals("Unexpected value for " + details, val, e.get(), 0d);

                    boolean expECaught = false;

                    final double newVal = val * 2.0;

                    try {
                        e.set(newVal);
                    } catch (UnsupportedOperationException uoe) {
                        if (!(readOnly(m)))
                            throw uoe;

                        expECaught = true;
                    }

                    if (readOnly(m)) {
                        if (!expECaught)
                            fail("Expected exception was not caught for " + details);

                        continue;
                    }

                    assertEquals("Unexpected value set for " + details, newVal, m.get(i, j), 0d);
                }
        });
    }

    /** */
    @Test
    public void testGetMetaStorage() {
        consumeSampleMatrix((m, desc) -> assertNotNull("Null meta storage in " + desc, m.getMetaStorage()));
    }

    /** */
    @Test
    public void testGuid() {
        consumeSampleMatrix((m, desc) -> assertNotNull("Null guid in " + desc, m.guid()));
    }

    /** */
    @Test
    public void testSwapRows() {
        consumeSampleMatrix((m, desc) -> {
            if (readOnly(m))
                return;

            double[][] doubles = fillAndReturn(m);

            final int swap_i = m.rowSize() == 1 ? 0 : 1;
            final int swap_j = 0;

            Matrix swap = m.swapRows(swap_i, swap_j);

            for (int col = 0; col < m.columnSize(); col++) {
                assertEquals("Unexpected value for " + desc + " at col " + col + ", swap_i " + swap_i,
                    swap.get(swap_i, col), doubles[swap_j][col], 0d);

                assertEquals("Unexpected value for " + desc + " at col " + col + ", swap_j " + swap_j,
                    swap.get(swap_j, col), doubles[swap_i][col], 0d);
            }

            testInvalidRowIndex(() -> m.swapRows(-1, 0), desc + " negative first swap index");
            testInvalidRowIndex(() -> m.swapRows(0, -1), desc + " negative second swap index");
            testInvalidRowIndex(() -> m.swapRows(m.rowSize(), 0), desc + " too large first swap index");
            testInvalidRowIndex(() -> m.swapRows(0, m.rowSize()), desc + " too large second swap index");
        });
    }

    /** */
    @Test
    public void testSwapColumns() {
        consumeSampleMatrix((m, desc) -> {
            if (readOnly(m))
                return;

            double[][] doubles = fillAndReturn(m);

            final int swap_i = m.columnSize() == 1 ? 0 : 1;
            final int swap_j = 0;

            Matrix swap = m.swapColumns(swap_i, swap_j);

            for (int row = 0; row < m.rowSize(); row++) {
                assertEquals("Unexpected value for " + desc + " at row " + row + ", swap_i " + swap_i,
                    swap.get(row, swap_i), doubles[row][swap_j], 0d);

                assertEquals("Unexpected value for " + desc + " at row " + row + ", swap_j " + swap_j,
                    swap.get(row, swap_j), doubles[row][swap_i], 0d);
            }

            testInvalidColIndex(() -> m.swapColumns(-1, 0), desc + " negative first swap index");
            testInvalidColIndex(() -> m.swapColumns(0, -1), desc + " negative second swap index");
            testInvalidColIndex(() -> m.swapColumns(m.columnSize(), 0), desc + " too large first swap index");
            testInvalidColIndex(() -> m.swapColumns(0, m.columnSize()), desc + " too large second swap index");
        });
    }

    /** */
    @Test
    public void testSetRow() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int rowIdx = m.rowSize() / 2;

            double[] newValues = new double[m.columnSize()];

            for (int i = 0; i < newValues.length; i++)
                newValues[i] = newValues.length - i;

            m.setRow(rowIdx, newValues);

            for (int col = 0; col < m.columnSize(); col++)
                assertTrue("Unexpected value for " + desc + " at " + col,
                    Double.compare(m.get(rowIdx, col), newValues[col]) == 0);
        });
    }

    /** */
    @Test
    public void testSetColumn() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int colIdx = m.columnSize() / 2;

            double[] newValues = new double[m.rowSize()];

            for (int i = 0; i < newValues.length; i++)
                newValues[i] = newValues.length - i;

            m.setColumn(colIdx, newValues);

            for (int row = 0; row < m.rowSize(); row++)
                assertTrue("Unexpected value for " + desc + " at " + row,
                    Double.compare(m.get(row, colIdx), newValues[row]) == 0);
        });
    }

    /** */
    @Test
    public void testViewPart() {
        consumeSampleMatrix((m, desc) -> {
            if (ignore(m.getClass()))
                return;

            fillMatrix(m);

            int rowOff = m.rowSize() < 3 ? 0 : 1;
            int rows = m.rowSize() < 3 ? 1 : m.rowSize() - 2;
            int colOff = m.columnSize() < 3 ? 0 : 1;
            int cols = m.columnSize() < 3 ? 1 : m.columnSize() - 2;

            Matrix view1 = m.viewPart(rowOff, rows, colOff, cols);
            Matrix view2 = m.viewPart(new int[] {rowOff, colOff}, new int[] {rows, cols});

            String details = desc + " view [" + rowOff + ", " + rows + ", " + colOff + ", " + cols + "]";

            for (int i = 0; i < rows; i++)
                for (int j = 0; j < cols; j++) {
                    assertTrue("Unexpected view1 value for " + details + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i + rowOff, j + colOff), view1.get(i, j)) == 0);

                    assertTrue("Unexpected view1 value for " + details + " at (" + i + "," + j + ")",
                        Double.compare(m.get(i + rowOff, j + colOff), view2.get(i, j)) == 0);
                }
        });
    }

    /** */
    @Test
    public void testDensity() {
        consumeSampleMatrix((m, desc) -> {
            if (!readOnly(m))
                fillMatrix(m);

            assertTrue("Unexpected density with threshold 0.", m.density(0.0));

            // TODO find out why m.density(1.0) appears to run infinitely.
        });
    }

    /** */
    private void testInvalidRowIndex(Supplier<Matrix> supplier, String desc) {
        try {
            supplier.get();
        } catch (RowIndexException | IndexException ie) {
            return;
        }

        fail("Expected exception was not caught for " + desc);
    }

    /** */
    private void testInvalidColIndex(Supplier<Matrix> supplier, String desc) {
        try {
            supplier.get();
        } catch (ColumnIndexException | IndexException ie) {
            return;
        }

        fail("Expected exception was not caught for " + desc);
    }

    /** */
    private void testMapMatrixWrongCardinality(Matrix m, String desc) {
        for (int rowDelta : new int[] {-1, 0, 1})
            for (int colDelta : new int[] {-1, 0, 1}) {
                if (rowDelta == 0 && colDelta == 0)
                    continue;

                int rowNew = m.rowSize() + rowDelta;
                int colNew = m.columnSize() + colDelta;

                if (rowNew < 1 || colNew < 1)
                    continue;

                try {
                    m.map(new DenseLocalOnHeapMatrix(rowNew, colNew), (m1, m2) -> m1 + m2);
                } catch (CardinalityException ce) {
                    continue;
                }

                fail("Expected exception was not caught on mapping wrong cardinality " + desc
                    + " mapping to size " + rowNew + "x" + colNew);
            }
    }

    /** */
    private boolean readOnly(Matrix m) {
        return m instanceof RandomMatrix;
    }

    /** */
    private double[][] fillIntAndReturn(Matrix m) {
        double[][] data = new double[m.rowSize()][m.columnSize()];

        if (readOnly(m)) {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = m.get(i, j);

        } else {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = i * m.rowSize() + j + 1;

            m.assign(data);
        }
        return data;
    }

    /** */
    private double[][] fillAndReturn(Matrix m) {
        double[][] data = new double[m.rowSize()][m.columnSize()];

        if (readOnly(m)) {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = m.get(i, j);

        } else {
            for (int i = 0; i < m.rowSize(); i++)
                for (int j = 0; j < m.columnSize(); j++)
                    data[i][j] = -0.5d + Math.random();

            m.assign(data);
        }
        return data;
    }

    /** */
    private void fillMatrix(Matrix m) {
        for (int i = 0; i < m.rowSize(); i++)
            for (int j = 0; j < m.columnSize(); j++)
                m.set(i, j, Math.random());
    }

    /** Ignore test for given matrix type. */
    private boolean ignore(Class<? extends Matrix> clazz) {
        boolean isIgnored = false;
        List<Class<? extends Matrix>> ignoredClasses = Arrays.asList(RandomMatrix.class, PivotedMatrixView.class);

        for (Class<? extends Matrix> ignoredClass : ignoredClasses) {
            if (ignoredClass.isAssignableFrom(clazz)) {
                isIgnored = true;
                break;
            }
        }

        return isIgnored;
    }

    /** */
    private static Map<Class<? extends Matrix>, Class<? extends Vector>> typesMap() {
        return new LinkedHashMap<Class<? extends Matrix>, Class<? extends Vector>>() {{
            put(DenseLocalOnHeapMatrix.class, DenseLocalOnHeapVector.class);
            put(DenseLocalOffHeapMatrix.class, DenseLocalOffHeapVector.class);
            put(RandomMatrix.class, RandomVector.class);
            // IMPL NOTE check for presence of all implementations here will be done in testHaveLikeMatrix via Fixture
        }};
    }
}
