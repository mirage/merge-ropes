open Lwt
open Core_kernel.Std

module Mem = IrminMemory.AO
module Key = IrminKey.SHA1

module Str = struct
  include IrminContents.String

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


module Rope = Merge_rope.Make(Mem)(Key)(Str)


module type S = sig
  type state
  type clue

  type action = SetGet | Insert | Delete | Append | Split

  val create : int -> action -> state * string

  val next_clue : state -> (clue * state) option
  val get_str : state -> clue -> string
  val get_len : state -> clue -> int
  val get_pos : state -> clue -> int

  val verify : state -> string -> bool
end


module Oracle : S = struct

  module Map = Int.Map

  type action = SetGet | Insert | Delete | Append | Split

  type clue = {
    rnk : int;
    pos : int;
    str : String.t;
    len : int;
  }

  type state = {
    act : action;
    lins : clue list;
    ldel : clue list;
    map : (bool * clue) Map.t;
  }


  let empty_clue = { rnk = 0; pos = 0; str = ""; len = 0 }
  let make_clue rnk pos str = { rnk; pos; str ; len = String.length str }

  let make_state act lins ldel map = { act; lins; ldel; map }


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

  let fusion list =
    let rec fusion_rec acc = function
      | [] -> List.rev acc
      | [a] -> List.rev (a::acc)
      | a::b::l ->
        if (Random.bool () || Random.bool ()) then
          fusion_rec acc ((Str.append a b)::l)
        else
          fusion_rec (a::acc) (b::l)
    in
    fusion_rec [] list
(*
     let make_str i c = String.make (i/2 + Random.int i) c
     let make_str i c = String.make (1 + Random.int i) c

     let make_strs _ =
     let rec make_rec k lim lst map =
     if k < lim then (lst, map)
     else
     let s = make_str 8 (char_of_int k) in
     let lst = s::lst in
     let map = Map.add map (k - lim) s in
     make_rec (k - 1) lim lst map
     in
     make_rec 122 97 [] Map.empty
  *)

  let create n act =
    let l = fusion (make_strs n) in
    let m = List.foldi l ~f:(fun i m str -> Map.add m i str) ~init:Map.empty in
    let map_false = Map.mapi
        ~f:(let pos = ref 0 in
            fun ~key ~data ->
              let clue = make_clue key (!pos) data in
              pos := (!pos) + (clue.len); (false, clue)
           )  m in
    let map_true = Map.mapi
        ~f:(let pos = ref 0 in
            fun ~key ~data ->
              let clue = make_clue key (!pos) data in
              pos := (!pos) + (clue.len); (true, clue)
           )  m in
    let list = List.mapi
        ~f:(let pos = ref 0 in
            fun i str ->
              let clue = make_clue i (!pos) str in
              pos := (!pos) + (clue.len); clue
           ) l in
    match act with
    | SetGet ->
      let state = make_state SetGet [] [] map_true in
      state, (String.concat l)
    | Insert ->
      let state = make_state act (List.permute list) [] map_false in
      state, ""
    | Delete ->
      let state = make_state act [] (List.permute list) map_true in
      state, (String.concat l)
    | Append ->
      let state = make_state act list [] map_false in
      state, ""
    | Split ->
      let state = make_state act [] list map_true in
      state, (String.concat l)


  let next_clue state =
    let act = state.act in
    let lins = state.lins in
    let ldel = state.ldel in
    let map = state.map in
    match act with
    | SetGet -> None
    | Insert
    | Append -> (
        match lins with
        | [] -> None
        | a::lins ->
          let ldel = a::ldel in
          let map = Map.add map (a.rnk) (true, a) in
          Some (a, {act; lins; ldel; map})
      )
    | Delete
    | Split -> (
        match ldel with
        | [] -> None
        | a::ldel ->
          let lins = a::lins in
          let map = Map.add map (a.rnk) (false, a) in
          Some (a, {act; lins; ldel; map})
      )

  let get_str state clue = clue.str
  let get_len state clue = clue.len
  let get_pos state clue =
    let map = Map.filter state.map ~f:(fun ~key ~data:(b, c) -> b && key < clue.rnk) in
    Map.fold map ~init:0 ~f:(fun ~key ~data:(b, c) pos -> pos + c.len)

  let verify state string =
    match state.act with
    | SetGet ->
      let str = Map.fold state.map ~init:"" ~f:(fun ~key ~data:(b, c) str -> Printf.sprintf "%s%s" str c.str) in
      String.(string = str)
    | Insert
    | Append ->
      let str = Map.fold state.map ~init:"" ~f:(fun ~key ~data:(b, c) str -> Printf.sprintf "%s%s" str c.str) in
      String.(string = str)
    | Delete
    | Split -> String.(string = "")
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





let setget len =
  let rec setget_rec sget sset rget rset = function
    | 0 -> return (sset, rset)
    | n -> (
        let cs = Str.get sget (n - 1) in
        let sset = Str.set sset (n - 1) cs in
        Rope.get rget (n - 1) >>= fun cr ->
        Rope.set rset (n - 1) cr >>= fun rset ->
        assert (cs = cr);
        setget_rec sget sset rget rset (n - 1)
      )
  in
  let (state, string) = Oracle.create len Oracle.SetGet in
  let len = String.length string in
  let sget = String.copy string in
  let sset = String.make len ' ' in
  Rope.make sget >>= fun rget ->
  Rope.make sset >>= fun rset ->
  setget_rec sget sset rget rset len >>= fun (string, rope) ->
  Rope.flush rope >>= fun srope ->
  assert (Oracle.verify state srope && Oracle.verify state string && String.(srope = string));
  return ()





let insert_string state string =
  let rec insert_rec state string =
    match Oracle.next_clue state with
    | None -> string
    | Some (clue, state) ->
      let str = Oracle.get_str state clue in
      let pos = Oracle.get_pos state clue in
      let string = Str.insert string pos str in
      (*print_endline (Printf.sprintf "(%i, %s) -> %s%!" pos str string);*)
      insert_rec state string
  in
  set_sclk ();
  let string = insert_rec state string in
  upd_sclk (); string

let insert_rope state string =
  let rec insert_rec state rope =
    (*print_endline "New insert";*)
    match Oracle.next_clue state with
    | None -> return rope
    | Some (clue, state) ->
      let str = Oracle.get_str state clue in
      let pos = Oracle.get_pos state clue in
      Rope.insert rope pos str >>= fun rope ->
      (*Rope.flush rope >>= fun string ->
        print_endline (Printf.sprintf "(%i, %s) -> %s%!" pos str string);*)
      insert_rec state rope
  in
  Rope.make string >>= fun rope ->
  set_rclk ();
  insert_rec state rope >>= fun rope ->
  upd_rclk (); return rope





let delete_string state string =
  let rec delete_rec state string =
    match Oracle.next_clue state with
    | None -> string
    | Some (clue, state) ->
      let pos = Oracle.get_pos state clue in
      let len = Oracle.get_len state clue in
      let string = Str.delete string pos len in
      (*print_endline (Printf.sprintf "(%i, %i) -> %s%!" pos len string);*)
      delete_rec state string
  in
  set_sclk ();
  let string = delete_rec state string in
  upd_sclk (); string

let delete_rope state string =
  let rec delete_rec state rope =
    (*print_endline "New delete";*)
    match Oracle.next_clue state with
    | None -> return rope
    | Some (clue, state) ->
      let pos = Oracle.get_pos state clue in
      let len = Oracle.get_len state clue in
      Rope.delete rope pos len >>= fun rope ->
      (*Rope.flush rope >>= fun string ->
        print_endline (Printf.sprintf "(%i, %i) -> %s%!" pos len string);*)
      delete_rec state rope
  in
  Rope.make string >>= fun rope ->
  set_rclk ();
  delete_rec state rope >>= fun rope ->
  upd_rclk (); return rope





let append_string state string =
  let rec append_rec state string =
    match Oracle.next_clue state with
    | None -> string
    | Some (clue, state) ->
      let str = Oracle.get_str state clue in
      let string = Str.append string str in
      append_rec state string
  in
  set_sclk ();
  let string = append_rec state string in
  upd_sclk (); string

let append_rope state string =
  let rec append_rec state rope =
    (*print_endline "New append";*)
    match Oracle.next_clue state with
    | None -> return rope
    | Some (clue, state) ->
      let str = Oracle.get_str state clue in
      Rope.make str >>= fun rpe ->
      Rope.append rope rpe >>= fun rope ->
      append_rec state rope
  in
  Rope.make string >>= fun rope ->
  set_rclk ();
  append_rec state rope >>= fun rope ->
  upd_rclk (); return rope





let split_string state string =
  let rec split_rec state string =
    match Oracle.next_clue state with
    | None -> string
    | Some (clue, state) ->
      let len = Oracle.get_len state clue in
      let str = Oracle.get_str state clue in
      let (string1, string2) = Str.split string len in
      assert String.(string1 = str);
      split_rec state string2
  in
  set_sclk ();
  let string = split_rec state string in
  upd_sclk (); string

let split_rope state string =
  let rec split_rec state rope =
    (*print_endline "New split";*)
    match Oracle.next_clue state with
    | None -> return rope
    | Some (clue, state) ->
      let len = Oracle.get_len state clue in
      let str = Oracle.get_str state clue in
      Rope.split rope len >>= fun (rope1, rope2) ->
      Rope.flush rope1 >>= fun string ->
      assert String.(string = str);
      split_rec state rope2
  in
  Rope.make string >>= fun rope ->
  set_rclk ();
  split_rec state rope >>= fun rope ->
  upd_rclk (); return rope




let main () =
  Random.self_init ();

  let len = 256 in
  let nbr = 256 in

  let rec iter_ins len = function
    | 0 -> return ()
    | n ->
      let (state, string) = Oracle.create len Oracle.Insert in
      let str = insert_string state string in
      insert_rope state string >>= fun rope ->
      Rope.flush rope >>= fun srope ->
      Rope.length rope >>= fun lrope ->
      assert (lrope = String.length str);
      assert (Oracle.verify state str && Oracle.verify state srope && String.(str = srope));
      (*Printf.printf "%-8i\r%!" n;*)
      iter_ins len (n - 1)
  in

  let rec iter_del len = function
    | 0 -> return ()
    | n ->
      let (state, string) = Oracle.create len Oracle.Delete in
      let str = delete_string state string in
      delete_rope state string >>= fun rope ->
      Rope.flush rope >>= fun srope ->
      Rope.length rope >>= fun lrope ->
      assert (lrope = String.length str);
      assert (Oracle.verify state str && Oracle.verify state srope && String.(str = srope));
      (*Printf.printf "%-8i\r%!" n;*)
      iter_del len (n - 1)
  in

  let rec iter_app len = function
    | 0 -> return ()
    | n ->
      let (state, string) = Oracle.create len Oracle.Append in
      let str = append_string state string in
      append_rope state string >>= fun rope ->
      Rope.flush rope >>= fun srope ->
      Rope.length rope >>= fun lrope ->
      assert (lrope = String.length str);
      assert (Oracle.verify state str && Oracle.verify state srope && String.(str = srope));
      (*Printf.printf "%-8i\r%!" n;*)
      iter_app len (n - 1)
  in

  let rec iter_spl len = function
    | 0 -> return ()
    | n ->
      let (state, string) = Oracle.create len Oracle.Split in
      let str = split_string state string in
      split_rope state string >>= fun rope ->
      Rope.flush rope >>= fun srope ->
      Rope.length rope >>= fun lrope ->
      assert (lrope = String.length str);
      assert (String.(str = srope));
      (*Printf.printf "%-8i\r%!" n;*)
      iter_spl len (n - 1)
  in
  setget len >>= fun () ->
  iter_ins len nbr >>= fun () ->
  iter_del len nbr >>= fun () ->
  iter_app len nbr >>= fun () ->
  iter_spl len nbr >>= fun () ->
  let stats = Rope.stats () in
  print_endline "Set:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.set);
  print_endline "Get:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.get);
  print_endline "Insert:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.insert);
  print_endline "Delete:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.delete);
  print_endline "Append:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.append);
  print_endline "Split:";
  print_endline (Merge_rope.string_of_statlist stats.Merge_rope.split);
  print_endline "Time:";
  print_endline (Printf.sprintf "%-8.2f%-8.2f" (get_sclk ()) (get_rclk ()));
  return ()


let () =
  Lwt_unix.run (main ())
