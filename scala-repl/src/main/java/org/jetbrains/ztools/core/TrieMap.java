/**
 * Copyright 2020 Jetbrains s.r.o.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetbrains.ztools.core;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TrieMap<T> {
    public static class Node<T> {
        T value;
        Map<String, Node<T>> children;

        Node(T value) {
            this.value = value;
        }

        void put(String key, Node<T> node) {
            if (children == null) children = new HashMap<>();
            children.put(key, node);
        }

        public void del(String key) {
            children.remove(key);
        }

        public void forEach(Function<T, ?> func) {
            func.apply(value);
            if (children != null) children.forEach((key, node) -> node.forEach(func));
        }
    }

    Node<T> root = new Node<>(null);

    public Node<T> subtree(String[] key, int length) {
        Node<T> current = root;

        for (int i = 0; i < length && current != null; i++) {
            if (current.children == null) return null;
            current = current.children.get(key[i]);
        }

        return current;
    }

    public void put(String[] key, T value) {
        Node<T> node = subtree(key, key.length - 1);
        node.put(key[key.length - 1], new Node<>(value));
    }

    public void put(String key, T value) {
        String[] k = split(key);
        put(k, value);
    }

    public boolean contains(String key) {
        String[] k = split(key);
        Node<T> node = subtree(k, k.length);
        return node != null;
    }

    static String[] split(String key) {
        int n = 0, j = 0;
        for (int i = 0; i < key.length(); i++) {
            if (key.charAt(i) == '.') n++;
        }
        String[] k = new String[n + 1];
        StringBuilder sb = new StringBuilder(k.length);
        for (int i = 0; i < key.length(); i++) {
            char ch = key.charAt(i);
            if (ch == '.') {
                k[j++] = sb.toString();
                sb.setLength(0);
            } else sb.append(ch);
        }
        k[j] = sb.toString();
        return k;
    }

    public T get(String key) {
        String[] k = split(key);
        Node<T> node = subtree(k, k.length);
        if (node == null) return null;
        return node.value;
    }

    public Node<T> subtree(String key) {
        String[] k = split(key);
        return subtree(k, k.length);
    }
}
