(*
 * Copyright (c) 2013-2014 Thomas Gazagnaire <thomas@gazagnaire.org>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*)

open Lwt
open Core_kernel.Std

module Log = Log.Make(struct let section = "QUEUE" end)


type stat = {
  ops : int;
  reads : int;
  writes : int;
}

type stats = {
  set : (int * stat) list;
  get : (int * stat) list;
  insert : (int * stat) list;
  delete : (int * stat) list;
  append : (int * stat) list;
  split : (int * stat) list;
  merge : (int * stat) list;
}

let string_of_stat t =
  Printf.sprintf "%-8i%-8.2f%-8.2f%!"
    t.ops
    ((float t.reads) /. (float t.ops))
    ((float t.writes) /. (float t.ops))

let string_of_statlist t =
  let tab = "->      " in
  List.fold ~init:"" ~f:(fun str (i, stat) -> Printf.sprintf "%s%-8i%s%s%s" str i tab (string_of_stat stat) "\n") t


module type S = sig
  include IrminContents.S
  type value
  type cont

  val create : unit -> t Lwt.t
  val make : cont -> t Lwt.t
  val flush : t -> cont Lwt.t

  val is_empty : t -> bool Lwt.t
  val length : t -> int Lwt.t

  val set : t -> pos:int -> value -> t Lwt.t
  val get : t -> pos:int -> value Lwt.t
  val insert : t -> pos:int -> cont -> t Lwt.t
  val delete : t -> pos:int -> len:int -> t Lwt.t
  val append : t -> t -> t Lwt.t
  val concat : sep:t -> t list -> t Lwt.t
  val split : t -> pos:int -> (t * t) Lwt.t

  val stats : unit -> stats
  val reset : unit -> unit
end




module Make
    (AO: Irmin.AO_MAKER)
    (K: IrminKey.S)
    (V :
     sig
       type a
       type t
       include IrminContents.S with type t := t

       val empty : t

       val length : t -> int       
       val set : t -> int -> a -> t
       val get : t -> int -> a
       val insert : t -> int -> t -> t
       val delete : t -> int -> int -> t
       val append : t -> t -> t
       val concat : t -> t list -> t
       val split : t -> int -> (t * t)
     end)
  : S with type value = V.a
       and type cont = V.t = struct


  (*
   * Type of a branch in the internal tree.
   * 'key' is the Irmin key of the subtree pointed to the branch,
   * 'min_depth' is the minimal depth of this subtree,
   * 'max_depth' the maximal one.
  *)
  type branch = {
    key : K.t;
    min_depth : int;
    max_depth : int;
  } with compare, sexp

  (*
   * Type of a node in the internal tree.
   * 'ind' is the index of the node, which is equal to length of the left subtree
   * 'len' is the length of the tree having this node as the root,
   * 'left' is the branch pointed to the left subtree,
   * 'right' the branch of the right one.
  *)
  type node = {
    ind : int;
    len : int;
    left : branch;
    right : branch;
  } with compare, sexp

  (*
   * Type of index, which is the accessor to the rope internal tree.
   * 'length' is the length of the whole rope,
   * 'root' is the root of the internal tree.
  *)
  type index = {
    length : int;
    root : K.t;
  } with compare, sexp
  type elt = Index of index | Node of node | Leaf of V.t with compare, sexp

  module C = struct
    module X = IrminIdent.Make(struct
        type nonrec t = elt
        with compare, sexp
      end)
    include X
  end

  module Store = struct

    (*
     * Enhance functions of the IrminStore with statistic tracking.
     * Each IrminRope function has its own table which store the number of read and write.
     * These tables are indexed by an 'int' which is the size of the rope.
    *)

    module Table = Int.Table

    type action = Set | Get | Insert | Delete | Append | Split | Merge | Other
    type ref_stats = {
      r_ops : int ref;
      r_reads : int ref;
      r_writes : int ref;
    }

    let t_set = Table.create ()
    let t_get = Table.create ()
    let t_insert = Table.create ()
    let t_delete = Table.create ()
    let t_concat = Table.create ()
    let t_split = Table.create ()
    let t_merge = Table.create ()
    let t_other = Table.create ()

    let switch action n =
      let table = match action with
        | Set -> t_set
        | Get -> t_get
        | Insert -> t_insert
        | Delete -> t_delete
        | Append -> t_concat
        | Split -> t_split
        | Merge -> t_merge
        | Other -> t_other
      in
      match Table.find table n with
      | None ->
        let empty = { r_ops = ref 0; r_reads = ref 0; r_writes = ref 0 } in
        Table.add_exn table n empty;
        empty
      | Some stat -> stat

    let get action =
      let table = match action with
        | Set -> t_set
        | Get -> t_get
        | Insert -> t_insert
        | Delete -> t_delete
        | Append -> t_concat
        | Split -> t_split
        | Merge -> t_merge
        | Other -> t_other
      in
      let list = List.map (Table.to_alist table)
          (fun (k, s) -> (k, { ops = !(s.r_ops); reads = !(s.r_reads); writes = !(s.r_writes) })) in
      List.sort ~cmp:(fun (k1, s1) (k2, s2) -> Int.compare k1 k2) list

    let (switch, incr_read, incr_write) =
      let count_ops = ref (ref 0) in
      let count_read = ref (ref 0) in
      let count_write = ref (ref 0) in
      (
        (fun action n ->
           let stat = switch action n in
           count_ops := stat.r_ops;
           count_read := stat.r_reads;
           count_write := stat.r_writes;
           incr (!count_ops);
        ),
        (fun () -> incr (!count_read)),
        (fun () -> incr (!count_write))
      )

    let reset () =
      Table.clear t_set;
      Table.clear t_get;
      Table.clear t_insert;
      Table.clear t_delete;
      Table.clear t_concat;
      Table.clear t_split;
      Table.clear t_merge;
      Table.clear t_other

    module S = AO(K)(C)
    include S

    let read t k =
      incr_read ();
      S.read t k

    let read_exn t k =
      incr_read ();
      S.read_exn t k

    let read_free t k =
      S.read_exn t k

    let add t v =
      incr_write ();
      S.add t v

  end



  let min_depth t = 1 + (min t.left.min_depth t.right.min_depth)
  let max_depth t = 1 + (max t.left.max_depth t.right.max_depth)
  let get_length = function
    | Index index -> index.length
    | Node node -> node.len
    | Leaf leaf -> V.length leaf



  (*
   * Following several construction functions.
   * The keyword 'create' means that a write is used during the construction.
   * Reversely, the keyword 'constr' signifies that no write is used.
  *)

  let create_branch store t =
    match t with
    | Leaf leaf ->
      Store.add store t >>= fun key ->
      return { key; min_depth = 1; max_depth = 1}
    | Node node ->
      Store.add store t >>= fun key ->
      return { key; min_depth = min_depth node; max_depth = max_depth node }
    | Index index -> assert false

  let constr_branch t key =
    match t with
    | Leaf leaf ->
      return { key; min_depth = 1; max_depth = 1 }
    | Node node ->
      return { key; min_depth = min_depth node; max_depth = max_depth node }
    | Index index -> assert false


  let make_node l_length r_length b_left b_right =
    assert (l_length >= 0);
    assert (r_length >= 0);
    return {
      ind = l_length;
      len = l_length + r_length;
      left = b_left;
      right = b_right
    }

  let create_node store t_left t_right =
    let l_length = get_length t_left in
    let	r_length = get_length t_right in
    create_branch store t_left >>= fun b_left ->
    create_branch store t_right >>= fun b_right ->
    make_node l_length r_length b_left b_right

  let constr_node (t_left, k_left) (t_right, k_right) =
    let l_length = get_length t_left in
    let	r_length = get_length t_right in
    constr_branch t_left k_left >>= fun b_left ->
    constr_branch t_right k_right >>= fun b_right ->
    make_node l_length r_length b_left b_right

  let crt_cns_node store t_left (t_right, k_right) =
    let l_length = get_length t_left in
    let	r_length = get_length t_right in
    create_branch store t_left >>= fun b_left ->
    constr_branch t_right k_right >>= fun b_right ->
    make_node l_length r_length b_left b_right

  let cns_crt_node store (t_left, k_left) t_right =
    let l_length = get_length t_left in
    let	r_length = get_length t_right in
    constr_branch t_left k_left >>= fun b_left ->
    create_branch store t_right >>= fun b_right ->
    make_node l_length r_length b_left b_right


  let create_index store t =
    match t with
    | Leaf leaf ->
      Store.add store t >>= fun key ->
      return { length = V.length leaf; root = key }
    | Node node ->
      Store.add store t >>= fun key ->
      return { length = node.len; root = key }
    | Index index -> return index

  let constr_index t key =
    match t with
    | Leaf leaf ->
      return { length = V.length leaf; root = key }
    | Node node ->
      return { length = node.len; root = key }
    | Index index -> return index


  (*
   * Internal tree rotation which is used to keep the tree balanced.
   * A tree rotation moves one node up in the tree and one node down. 
  *)
  let rec rotate =

    let left_to_right store t =
      match t with
      | Leaf _ -> assert false
      | Index _ -> assert false
      | Node node ->
        Store.read_exn store node.left.key >>= fun t_left ->
        Store.read_exn store node.right.key >>= fun t_right ->
        match t_left with
        | Leaf _ -> assert false
        | Index _ -> assert false
        | Node n_left ->
          Store.read_exn store n_left.left.key >>= fun t_lleft ->
          Store.read_exn store n_left.right.key >>= fun t_lright ->
          constr_node (t_lright, n_left.right.key) (t_right, node.right.key) >>= fun new_right ->
          rotate store (Node new_right) >>= fun new_right ->
          cns_crt_node store (t_lleft, n_left.left.key) (Node new_right)
    in

    let right_to_left store t =
      match t with
      | Leaf _ -> assert false
      | Index _  -> assert false
      | Node node ->
        Store.read_exn store node.left.key >>= fun t_left ->
        Store.read_exn store node.right.key >>= fun t_right ->
        match t_right with
        | Leaf _ -> assert false
        | Index _ -> assert false
        | Node n_right ->
          Store.read_exn store n_right.left.key >>= fun t_rleft ->
          Store.read_exn store n_right.right.key >>= fun t_rright ->
          constr_node (t_left, node.left.key) (t_rleft, n_right.left.key) >>= fun new_left ->
          rotate store (Node new_left) >>= fun new_left ->
          crt_cns_node store (Node new_left) (t_rright, n_right.right.key)
    in

    fun store t ->
      match t with
      | Leaf _ -> assert false
      | Index _ -> assert false
      | Node node ->
        (*print_endline (Printf.sprintf "Left: %i %i, Right; %i %i"
          node.left.min_depth node.left.max_depth node.right.min_depth node.right.max_depth);*)
        if (node.right.max_depth - node.left.min_depth > node.left.min_depth) then
          right_to_left store t
        else if (node.left.max_depth - node.right.min_depth > node.right.min_depth) then
          left_to_right store t
        else return node



  (*
   * Create a new empty rope.
  *)
  let create () =
    Store.create () >>= fun store ->
    create_index store (Leaf V.empty)


  (*
   * Create a new rope from a given container.
   * In order to reduce the number of read/write, the containers stored 
   * in the leafs have a length equal to depth of the tree.
  *)
  let (make, make_internal) =

    let rec make_rec store depth cont =
      let length = V.length cont in
      if (length < 2 * depth) then return (Leaf cont)
      else
        let (c_left, c_right) = V.split cont (length / 2) in
        make_rec store (depth + 1) c_left >>= fun t_left ->
        make_rec store (depth + 1) c_right >>= fun t_right ->
        create_node store t_left t_right >>= fun node ->
        return (Node node)
    in
    (
      (
        fun cont ->
          Store.switch Store.Other (V.length cont);
          Store.create () >>= fun store ->
          make_rec store 1 cont >>= fun t ->
          create_index store t
      ),
      (
        fun store ?depth:(d=1) cont ->
          make_rec store d cont
      )
    )


  (*
   * Flush the rope into a container.
  *)
  let (flush, flush_free, flush_internal) =

    let rec flush_rec store list = function
      | Index _ -> assert false
      | Leaf leaf -> return (leaf::list)
      | Node node ->
        Store.read_exn store node.left.key >>= fun t_left ->
        Store.read_exn store node.right.key >>= fun t_right ->
        flush_rec store list t_right >>= fun list ->
        flush_rec store list t_left
    in
    (
      (
        fun index ->
          Store.create () >>= fun store ->
          Store.read_exn store index.root >>= fun t ->
          flush_rec store [] t >>= fun list ->
          return (V.concat V.empty list)
      ),
      (
        fun index ->
          Store.switch Store.Other index.length;
          Store.create () >>= fun store ->
          Store.read_exn store index.root >>= fun t ->
          flush_rec store [] t >>= fun list ->
          return (V.concat V.empty list)
      ),
      (
        fun store ?list:(l=[]) t ->
          flush_rec store l t >>= fun list ->
          return (V.concat V.empty list)
      )
    )

  let is_empty index = return (index.length = 0)
  let length index = return index.length





  type choice = Left | Right

  (*
   * Set the value at the position 'i' in the rope to 'a'
  *)
  let set index i a =

    Store.switch Store.Set index.length;
    Store.create () >>= fun store ->

    let choose node i =
      if (node.len < i || i < 0) then
        invalid_arg (Printf.sprintf "try to access the position %i in a rope of size %i" i node.len)
      else
      if (i < node.ind) then Left else Right
    in

    let rec set_rec i a = function
      | Index _ -> assert false
      | Leaf leaf ->
        let leaf = V.set leaf i a in
        return (Leaf leaf)
      | Node node ->
        match choose node i with
        | Left ->
          Store.read_exn store node.left.key >>= fun t_left ->
          set_rec i a t_left >>= fun t_left ->
          create_branch store t_left >>= fun b_left ->
          make_node node.ind (node.len - node.ind) b_left node.right >>= fun node ->
          return (Node node)
        | Right ->
          Store.read_exn store node.right.key >>= fun t_right ->
          set_rec (i - node.ind) a t_right >>= fun t_right ->
          create_branch store t_right >>= fun b_right ->
          make_node node.ind (node.len - node.ind) node.left b_right >>= fun node ->
          return (Node node)
    in

    Store.read_exn store index.root >>= fun t ->
    set_rec i a t >>= fun t ->
    create_index store t


  (*
   * Return the value at the position 'i' in the rope.
  *)
  let get index i =

    Store.switch Store.Get index.length;
    Store.create () >>= fun store ->

    let choose node i =
      if (node.len < i || i < 0) then
        invalid_arg (Printf.sprintf "try to access the position %i in a rope of size %i" i node.len)
      else
      if (i < node.ind) then Left else Right
    in

    let rec get_rec i = function
      | Index _ -> assert false
      | Leaf leaf -> return (V.get leaf i)
      | Node node ->
        match choose node i with
        | Left ->
          Store.read_exn store node.left.key >>= fun t_left ->
          get_rec i t_left
        | Right ->
          Store.read_exn store node.right.key >>= fun t_right ->
          get_rec (i - node.ind) t_right
    in

    Store.read_exn store index.root >>= fun t ->
    get_rec i t



  (*
   * Insert the container 'cont' at the position 'i' in the rope.
   * Rebalancing is made from bottom to up when the rope is rebuild.
   * Maintain the 'tree depth/leaf length' invariant.
  *)
  let insert index i cont =

    Store.switch Store.Insert (index.length);
    Store.create () >>= fun store ->

    let len = V.length cont in

    let choose node i =
      if (node.len < i || i < 0) then
        invalid_arg (Printf.sprintf "try to access the position %i in a rope of size %i" i node.len)
      else
      if (i < node.ind) then Left else Right
    in

    let rec insert_rec depth i = function
      | Index _ -> assert false
      | Leaf leaf ->
        let leaf = V.insert leaf i cont in
        let l_length = V.length leaf in
        if (l_length < 2 * depth) then return (Leaf leaf)
        else
          make_internal store ~depth leaf
      | Node node ->
        match choose node i with
        | Left ->
          Store.read_exn store node.left.key >>= fun t_left ->
          insert_rec (depth + 1) i t_left >>= fun t_left ->
          create_branch store t_left >>= fun b_left ->
          make_node (node.ind + len) (node.len - node.ind) b_left node.right >>= fun node ->
          rotate store (Node node) >>= fun node ->
          return (Node node)
        | Right ->
          Store.read_exn store node.right.key >>= fun t_right ->
          insert_rec (depth + 1) (i - node.ind) t_right >>= fun t_right ->
          create_branch store t_right >>= fun b_right ->
          make_node node.ind (node.len - node.ind + len) node.left b_right >>= fun node ->
          rotate store (Node node) >>= fun node ->
          return (Node node)
    in

    Store.read_exn store index.root >>= fun t ->
    insert_rec 1 i t >>= fun t ->
    create_index store t



  (*
   * Delete the elements between the position 'i' and 'j'.
   * Rebalancing is made from bottom to up when the rope is rebuild.
   * Maintain the 'tree depth/leaf length' invariant.
  *)
  let delete index i j =

    Store.switch Store.Delete index.length;
    Store.create () >>= fun store ->

    let choose node i =
      if (node.len < i || i < 0) then
        invalid_arg (Printf.sprintf "try to access the position %i in a rope of size %i" i node.len)
      else
      if (i < node.ind) then Left else Right
    in

    let clean store depth = function
      | Index _ -> assert false
      | Leaf leaf -> return (Leaf leaf)
      | Node node ->
        if (node.ind < depth / 2 || node.len - node.ind < depth / 2) then
          flush_internal store (Node node) >>= fun cont ->
          make_internal store ~depth cont
        else
          rotate store (Node node)  >>= fun node ->
          return (Node node)
    in

    let rec delete_rec depth i = function
      | Index _ -> assert false
      | Leaf leaf ->
        let leaf = V.delete leaf i j in
        return (Leaf leaf)
      | Node node ->
        match (choose node i, choose node (i + j)) with
        | Left, Left ->
          Store.read_exn store node.left.key >>= fun t_left ->
          delete_rec (depth + 1) i t_left >>= fun t_left ->
          clean store depth t_left >>= fun t_left ->
          create_branch store t_left >>= fun b_left ->
          make_node (node.ind - j) (node.len - node.ind) b_left node.right >>= fun node ->
          rotate store (Node node) >>= fun node ->
          return (Node node)
        | Left, Right ->
          flush_internal store (Node node) >>= fun cont ->
          let cont = V.delete cont i j in
          make_internal store ~depth cont >>= fun t ->
          return t
        | Right, Left -> assert false
        | Right, Right ->
          Store.read_exn store node.right.key >>= fun t_right ->
          delete_rec (depth + 1) (i - node.ind) t_right >>= fun t_right ->
          clean store depth t_right >>= fun t_right ->
          create_branch store t_right >>= fun b_right ->
          make_node node.ind (node.len - node.ind - j) node.left b_right >>= fun node ->
          rotate store (Node node) >>= fun node ->
          return (Node node)
    in

    Store.read_exn store index.root >>= fun t ->
    delete_rec 1 i t >>= fun t ->
    create_index store t



  (*
   * Append the two given ropes. If the first rope is longer than the second,
   * then the second rope is inserted at the right most position in the first rope.
   * Else the first one is inserted at the left most position in the second rope.
  *)
  let (append, append_internal) =

    let rec append_left store t = function
      | Index _ -> assert false
      | Leaf leaf ->
        create_node store t (Leaf leaf) >>= fun node ->
        rotate store (Node node) >>= fun node ->
        return (Node node)
      | Node node ->
        Store.read_exn store node.left.key >>= fun t_left ->
        append_left store t t_left >>= fun t_left ->
        create_branch store t_left >>= fun b_left ->
        make_node (get_length t_left) (node.len - node.ind) b_left node.right >>= fun node ->
        rotate store (Node node) >>= fun node ->
        return (Node node)
    in

    let rec append_right store t = function
      | Index _ -> assert false
      | Leaf leaf ->
        create_node store (Leaf leaf) t >>= fun node ->
        rotate store (Node node) >>= fun node ->
        return (Node node)
      | Node node ->
        Store.read_exn store node.right.key >>= fun t_right ->
        append_right store t t_right >>= fun t_right ->
        create_branch store t_right >>= fun b_right ->
        make_node node.ind (get_length t_right) node.left b_right >>= fun node ->
        rotate store (Node node) >>= fun node ->
        return (Node node)
    in

    (
      (fun index1 index2 ->

         Store.switch Store.Append (index1.length + index2.length);
         Store.create () >>= fun store ->
         Store.read_exn store index1.root >>= fun t1 ->
         Store.read_exn store index2.root >>= fun t2 ->
         if (index1.length = 0) then return index2
         else if (index2.length = 0) then return index1
         else (
           if (index1.length < index2.length) then
             append_left store t1 t2
           else
             append_right store t2 t1
         ) >>= fun node ->
           create_index store node
      ),
      (fun store t1 t2 ->
         let l1 = get_length t1 in
         let l2 = get_length t2 in
         if (l1 = 0) then return t2
         else if (l2 = 0) then return t1
         else if (l1 < l2) then
           append_left store t1 t2
         else
           append_right store t2 t1
      )
    )


  let concat t list =

    Store.switch Store.Append (List.fold ~init:0 ~f:(fun i index -> i + index.length) list);
    flush t >>= fun t ->
    Lwt_list.map_s flush list >>= fun list ->
    let cont = V.concat t list in
    make cont



  (*
   * Split the rope at the position 'i'.
   * The internal tree is recursevely cut into two forest.
   * Then these forest are concatenated in order to obtain the two new ropes.
  *)
  let split index i =

    Store.switch Store.Split index.length;
    Store.create () >>= fun store ->
    create () >>= fun empty ->

    let choose node i =
      if (node.len < i || i < 0) then
        invalid_arg (Printf.sprintf "try to access the position %i in a rope of size %i" i node.len)
      else
      if (i < node.ind) then Left else Right
    in

    let rec split_rec i l_acc r_acc = function
      | Index _ -> assert false
      | Leaf leaf ->
        let (l_left, l_right) = V.split leaf i in
        append_internal store l_acc (Leaf l_left) >>= fun l_acc ->
        append_internal store (Leaf l_right) r_acc >>= fun r_acc ->
        return (l_acc, r_acc)
      | Node node ->
        Store.read_exn store node.left.key >>= fun t_left ->
        Store.read_exn store node.right.key >>= fun t_right ->
        match choose node i with
        | Left ->
          append_internal store t_right r_acc >>= fun r_acc ->
          split_rec i l_acc r_acc t_left
        | Right ->
          append_internal store l_acc t_left >>= fun l_acc ->
          split_rec (i - node.ind) l_acc r_acc t_right
    in

    Store.read_exn store index.root >>= fun t ->
    split_rec i (Leaf V.empty) (Leaf V.empty) t >>= fun (l_acc, r_acc) ->
    create_index store l_acc >>= fun t_left ->
    create_index store r_acc >>= fun t_right ->
    return (t_left, t_right)


  type rope = index with sexp, compare
  type value = V.a
  type cont = V.t

  module T = IrminIdent.Make(struct
      type t = rope with sexp, compare
    end)



  (*
   * Merge the given two ropes with the help of their common ancestor.
   * The algorithm tries to deduce the smallest subtree on which modifications occur,
   * and then applies the user provided merge function on it.
   * If the modifications appear in different subtrees, the merge is automatically solved,
   * without the call of the user merge function.
  *)
  let merge =

    let module OP = IrminMerge.OP in
    let merge_cont = IrminMerge.merge V.merge in

    let merge_flush store origin old t1 t2 =
      if (C.compare t1 t2 = 0) then OP.ok t1
      else
        flush_internal store old >>= fun old ->
        flush_internal store t1 >>= fun cont1 ->
        flush_internal store t2 >>= fun cont2 ->
        merge_cont ~origin ~old cont1 cont2 >>= fun res ->
        match res with
        | `Conflict s -> OP.conflict "%s" s
        | `Ok cont -> OP.ok (Leaf cont)
    in

    let merge_branch store old1 old2 ((b11, l11), (b12, l12)) ((b21, l21), (b22, l22)) =
      if (b11.key = b21.key) then (Some (b11, l11))
      else if (b12.key = b22.key) then (
        if (old1.key = b11.key) then (Some (b21, l21))
        else if (old1.key = b21.key) then (Some (b11, l11))
        else None
      )
      else if (b12.key = old2.key && b21.key = old1.key) then (Some (b11, l11))
      else if (b11.key = old1.key && b22.key = old2.key) then (Some (b21, l21))
      else None
    in

    let rec merge_node store origin old n1 n2 =
      let ln1 = (n1.left, n1.ind) in
      let rn1 = (n1.right, n1.len - n1.ind) in
      let ln2 = (n2.left, n2.ind) in
      let rn2 = (n2.right, n2.len- n2.ind) in
      let l_opt = merge_branch store old.left old.right (ln1, rn1) (ln2, rn2) in
      let r_opt = merge_branch store old.right old.left (rn1, ln1) (rn2, ln2) in
      match (l_opt, r_opt) with
      | None, None -> merge_flush store origin (Node old) (Node n1) (Node n2)
      | None, Some (br, lr) -> (
          Store.read_exn store br.key >>= fun t_right ->
          Store.read_exn store old.left.key >>= fun old ->
          Store.read_exn store n1.left.key >>= fun t1 ->
          Store.read_exn store n2.left.key >>= fun t2 ->
          merge_elt store origin old t1 t2 >>= fun res ->
          match res with
          | `Conflict s -> OP.conflict "%s" s
          | `Ok t_left ->
            crt_cns_node store t_left (t_right, br.key) >>= fun node ->
            OP.ok (Node node)
        )
      | Some (bl, ll), None -> (
          Store.read_exn store bl.key >>= fun t_left ->
          Store.read_exn store old.right.key >>= fun old ->
          Store.read_exn store n1.right.key >>= fun t1 ->
          Store.read_exn store n2.right.key >>= fun t2 ->
          merge_elt store origin old t1 t2 >>= fun res ->
          match res with
          | `Conflict s -> OP.conflict "%s" s
          | `Ok t_right ->
            cns_crt_node store (t_left, bl.key) t_right >>= fun node ->
            OP.ok (Node node)
        )
      | Some (bl, ll), Some (br, lr) ->
        make_node ll lr bl br >>= fun node ->
        OP.ok (Node node)

    and merge_elt store origin old t1 t2 =
      match (t1, t2) with
      | Index _, _
      | _, Index _ -> assert false
      | Leaf _, _
      | _, Leaf _ -> merge_flush store origin old t1 t2
      | Node node1, Node node2 ->
        match old with
        | Index _ -> assert false
        | Leaf _ -> merge_flush store origin old t1 t2
        | Node old -> merge_node store origin old node1 node2
    in

    let merge_rope ~origin ~old r1 r2 =
      Store.create () >>= fun store ->
      Store.switch Store.Merge old.length;
      Store.read_exn store old.root >>= fun old ->
      Store.read_exn store r1.root >>= fun t1 ->
      Store.read_exn store r2.root >>= fun t2 ->
      merge_elt store origin old t1 t2 >>= fun res ->
      match res with
      | `Conflict s -> OP.conflict "%s" s
      | `Ok t ->
        create_index store t >>= fun index ->
        OP.ok index
    in

    IrminMerge.create' (module T) merge_rope


  include T

  let create = create
  let make = make
  let flush = flush_free

  let is_empty = is_empty
  let length = length

  let set t ~pos = set t pos
  let get t ~pos = get t pos
  let insert t ~pos cont = insert t pos cont
  let delete t ~pos ~len = delete t pos len
  let append t1 t2 = append t1 t2
  let concat ~sep list = concat sep list
  let split t ~pos = split t pos

  let merge = merge

  let stats () = {
    set = Store.get Store.Set;
    get = Store.get Store.Get;
    insert = Store.get Store.Insert;
    delete = Store.get Store.Delete;
    append = Store.get Store.Append;
    split = Store.get Store.Split;
    merge = Store.get Store.Merge;
  }

  let reset () = Store.reset ()

end
