open Lwt
open Core_kernel.Std

module Mem = Irmin_git.AO(Git.Memory)
module Key = Irmin.Hash.SHA1
module Path = Irmin.Path.String_list

module Str = struct
  include Irmin.Contents.String

  type a = char

  let empty = ""
  let length = String.length

  let set t i a =
    let s = String.copy t in
    String.set s i a; s
  let get = String.get

  let insert t i s =
    assert Int.(0 <= i && i <= String.length t);
    let left = String.sub t 0 i in
    let right = String.sub t i (String.length t - i) in
    String.concat [left; s; right]

  let delete t i j =
    assert Int.(0 <= i && (i + j) <= String.length t);
    let left = String.sub t 0 i in
    let right = String.sub t (i + j) (String.length t - (i + j)) in
    String.concat [left; right]

  let append s t = String.concat [s; t]

  let concat sep list = String.concat ~sep list

  let split t i =
    assert Int.(0 <= i && i <= String.length t);
    let left = String.sub t 0 i in
    let right = String.sub t i (String.length t - i) in
    (left, right)
end

module Config = struct
  let conf = Irmin_git.config ()
  let task = Irmin_unix.task
end


module Rope = Merge_rope.Make(Mem)(Key)(Str)(Config)


module type S = sig
  type state
  type clue

  val create : int -> state * string

  val split : state -> clue -> state * state
  val merge :  state -> state -> state

  val next_clue : state -> (clue * state) option
  val get_str : state -> clue -> string
  val get_len : state -> clue -> int
  val get_pos : state -> clue -> int

  val verify : state -> string -> bool
end


module Oracle : S = struct

  module Map = Int.Map

  type clue = {
    rnk : int;
    pos : int;
    str : String.t;
    len : int;
  }

  type state = {
    list : clue list;
    map : (bool * clue) Map.t;
  }


  let empty_clue = { rnk = 0; pos = 0; str = ""; len = 0 }
  let make_clue rnk pos str = { rnk; pos; str ; len = String.length str }

  let make_state list map = { list; map }


  let make_strs n =
    let char_of_int n pos mask =
      char_of_int (33 + Int.shift_right (Int.bit_and n (Int.shift_left mask pos)) pos)
    in
    let rec make_rec lst = function
      | 0 -> lst
      | n ->
        let bits = Random.bits () in
        let c1 = char_of_int bits 0 63 in
        let c2 = char_of_int bits 6 63 in
        let c3 = char_of_int bits 12 63 in
        let c4 = char_of_int bits 18 63 in
        let c5 = char_of_int bits 24 63 in
        let str = String.of_char_list [c1;c2;c3;c4;c5] in
        let lst = str::lst in
        make_rec lst (n - 1)
    in
    make_rec [] n



(*
     let make_str i c = String.make (i/2 + Random.int i) c
     let make_str i c = String.make (1 + Random.int i) c
     let make_str i c = String.make 1 c

     let make_strs _ =
     let rec make_rec k lim lst map =
     if k < lim then (lst, map)
     else
     let s = make_str 8 (char_of_int k) in
     let lst = s::lst in
     let map = Map.add map (k - lim) s in
     make_rec (k - 1) lim lst map
     in
     (*make_rec 122 97 [] Map.empty*)
  *)


  let fusion list =
    let rec fusion_rec acc = function
      | [] -> List.rev acc
      | [a] -> List.rev (a::acc)
      | a::b::l ->
        if (Random.bool ()) then
          fusion_rec acc ((Str.append a b)::l)
        else
          fusion_rec (a::acc) (b::l)
    in
    fusion_rec [] list

  let select list map =
    let (lst1, lst2) = List.split_n (List.permute list) ((List.length list) * 9 / 10) in
    let map = List.fold lst1 ~init:map ~f:(fun map c -> Map.add map c.rnk (true, c)) in
    let str = Map.fold map ~init:"" ~f:(fun ~key ~data:(b, c) str -> if b then Printf.sprintf "%s%s" str c.str else str) in
    make_state lst2 map, str

  let create n =
    let l = fusion (make_strs n) in
    let m = List.foldi l ~f:(fun i m str -> Map.add m i str) ~init:Map.empty in
    let map = Map.mapi
        ~f:(let pos = ref 0 in
            fun ~key ~data ->
              let clue = make_clue key (!pos) data in
              pos := (!pos) + (clue.len); (false, clue)
           )  m in
    let list = List.mapi
        ~f:(let pos = ref 0 in
            fun i str ->
              let clue = make_clue i (!pos) str in
              pos := (!pos) + (clue.len); clue
           ) l in
    select list map


  let split state clue =
    let f = fun c -> c.pos < clue.pos in
    let (l1, l2) = List.partition_tf state.list f in
    let state1 = make_state l1 state.map in
    let state2 = make_state l2 state.map in
    (state1, state2)



  let merge state1 state2 =
    let f ~key = function
      | `Left _
      | `Right _ -> assert false
      | `Both ((b1, c1), (b2, c2)) ->
        assert (c1 = c2);
        Some (b1 || b2, c1)
    in
    let map = Map.merge state1.map state2.map f in
    let list = List.filter state1.list ~f:(fun c -> List.mem state2.list c) in
    make_state list map



  let next_clue state =
    let list = List.permute state.list in
    let map = state.map in
    match list with
    | [] -> None
    | a::list ->
      let map = Map.add map (a.rnk) (true, a) in
      Some (a, {list; map})



  let get_str state clue = clue.str
  let get_len state clue = clue.len
  let get_pos state clue =
    let map = Map.filter state.map ~f:(fun ~key ~data:(b, c) -> b && key < clue.rnk) in
    Map.fold map ~init:0 ~f:(fun ~key ~data:(b, c) pos -> pos + c.len)



  let verify state string =
    let str = Map.fold state.map ~init:"" ~f:(fun ~key ~data:(b, c) str -> Printf.sprintf "%s%s" str c.str) in
    print_endline string;
    print_endline str;
    String.(string = str)

end





let (set_sclk, get_sclk, upd_sclk, set_rclk, get_rclk, upd_rclk) =
  let s_sum = ref 0. in
  let r_sum = ref 0. in
  let s_tmp = ref 0. in
  let r_tmp = ref 0. in
  (
    (fun () -> s_tmp := (Sys.time ())),
    (fun () -> !s_sum),
    (fun () -> s_sum := (!s_sum) +. (Sys.time ()) -. (!s_tmp)),
    (fun () -> r_tmp := (Sys.time ())),
    (fun () -> !r_sum),
    (fun () -> r_sum := (!r_sum) +. (Sys.time ()) -. (!r_tmp))
  )


module OP = Irmin.Merge.OP

let merge state string =
  let rec merge_rec d state old =
    (*Rope.flush old >>= fun sold ->
      print_endline (Printf.sprintf "%i <- %s" d sold);*)
    match Oracle.next_clue state with
    | None ->  OP.ok old
    | Some (clue, state1) -> (
        let str = Oracle.get_str state1 clue in
        let pos = Oracle.get_pos state1 clue in
        Rope.insert old pos str >>= fun r1 ->
        (*Rope.flush r1 >>= fun str1 ->
          print_endline (Printf.sprintf "%i <- %s <- %s" d str str1);*)
        match Oracle.next_clue state with
        | None -> OP.ok r1
        | Some (clue, state2) -> (
            let str = Oracle.get_str state2 clue in
            let pos = Oracle.get_pos state2 clue in
            Rope.insert old pos str >>= fun r2 ->
            (*Rope.flush r2 >>= fun str2 ->
              print_endline (Printf.sprintf "%i <- %s <- %s" d str str2);*)
            let old () = Lwt.return @@ `Ok (Some (Some old)) in
            Rope.merge [] ~old (Some r1) (Some r2) >>= fun res ->
            match res with
            | `Conflict _ | `Ok None -> OP.conflict "merge"
            | `Ok (Some rope) ->
              (*Rope.flush rope >>= fun str ->
                print_endline (Printf.sprintf "%i -> %s" d str);*)
              merge_rec (d + 1) (Oracle.merge state1 state2) rope
          )
      )
  in
  Rope.make string >>= fun rope ->
  merge_rec 0 state rope


let main () =

  let len = 1024 in
  let nbr = 256 in
  Random.self_init ();

  let rec iter c = function
    | 0 -> return ()
    | n ->
      Printf.printf "%-8i->      %-8i%s%!" n c "\r";
      let (state, string) = Oracle.create len in
      merge state string >>= fun res ->
      match res with
      | `Conflict s -> iter (c + 1) (n - 1)
      | `Ok _ -> iter c (n - 1)
      (*Rope.flush rope >>= fun str ->
        assert (Oracle.verify state str);*)
  in

  iter 0 nbr >>= fun () ->
  let stats = Rope.stats () in
  print_endline "Merge:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.merge);
  return ()

let () =
  Lwt_unix.run (main ())
